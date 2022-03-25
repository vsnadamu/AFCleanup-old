#!/usr/bin/env python3
"""
Artifactory Cleanup
"""
from AFConfig import AFConfig
from AFInstance import AFInstance

import os
import logging
import csv
from datetime import datetime, timezone, timedelta
import contextlib
from tqdm import tqdm
import time
import mysql.connector
import platform
from collections import namedtuple, defaultdict
import argparse
import sys
#import logutil

assert sys.version_info >= (3, 9), "This script requires Python 3.9+. Did you forgot to run pyenv.sh script?"

AF_CLEANUP_VERSION = 'AF CLEANUP V1.0: '


class RetentionPropError(Exception):
    pass


class RetentionPropValueError(RetentionPropError):
    pass


class RetentionProNotFound(RetentionPropError):
    def __init__(self):
        super().__init__("Neither retention.days nor retension.pinned set")


class AFRepoPurger:

    def __init__(self, instance, repo):
        self.instance = instance
        self.repo = repo
        assert repo, 'repo must be a valid string'
        self.afi = AFInstance('config.json', instance)
        self.afcfg = AFConfig(instance)
        self.root = self.afi.joinpath(repo=self.repo)
        self.root_len = len(str(self.root))


    def get_retention_days(self, afp):
        """
        Returns days if OK,
        Raises exception if props do not have valid data
        """
        props = self.afi.get_propeties(afp)
        rd = props.get('retention.days')
        rp = props.get('retention.pinned')
        if not rd and not rp:
            raise RetentionProNotFound()

        rdi, rpi = -1, -1
        if rd:
            try:
                rdi = int(rd)
            except Exception as ex:
                raise RetentionPropValueError(f"Invalid retention.days value '{rd}' - {ex}")
        # retention.pinned is #of months to retain.
        if rp:
            try:
                rpi = int(rp) * 30
            except Exception as ex:
                raise RetentionPropValueError(f"Invalid retention.pinned value '{rp}' - {ex}")
        ret = max(rdi, rpi)
        if ret <= 0:
            raise RetentionPropValueError(
                f"calculated retention days is <= 0 ({ret}) - retention.days '{rd}' retention.pinned '{rp}'")

        return ret

    def sql_query(self, sql, params):
        logging.info("Connecting to DB...")
        with contextlib.closing(mysql.connector.connect(**self.afcfg.get_db_config('PRIMARY'))) as afdb:
            # logging.info("Connected")
            with contextlib.closing(afdb.cursor(named_tuple=True)) as cur:
                logging.info("Executing query")
                cur.execute(sql, params)
                # print(cur.rowcount, ' Rows affected')
                logging.info("Fetching rows")
                all_rows = cur.fetchall()
                afdb.commit()

                if self.repo == 'mountevans_sw_bsp-or-local':
                    logging.info("Rows before filtering: %d", len(all_rows))
                    # override the retention for builds/official/**/build_completed to 1065
                    all_rows = [row for row in all_rows if not (row.node_path.startswith(
                        'builds/official/') and row.node_name == 'build_completed' and row.age_days <= 1065)]
                    logging.info("Rows after filtering: %d", len(all_rows))
                return all_rows

    def get_batch(self, last_node_id, batch_size):
        params = [self.repo]
        node_id_clause = ""
        if last_node_id:
            params += [last_node_id]
            node_id_clause = " AND nodes.node_id > %s"

        sql = f'''
        SELECT 
          nodes.node_id, nodes.node_path, nodes.node_name
          , nodes.bin_length
          , (UNIX_TIMESTAMP()-GREATEST(nodes.created, nodes.modified, nodes.updated, IFNULL(stats.last_downloaded, 0))/1000)/86400 as age_days
          , CAST(node_props.prop_value AS SIGNED INTEGER) as retention_days
        FROM nodes 
          LEFT JOIN node_props ON (nodes.node_id = node_props.node_id)
          LEFT JOIN stats ON (nodes.node_id = stats.node_id)
        WHERE 
          nodes.repo = %s
          AND nodes.node_type = 1		
          AND node_props.prop_key = 'retention.days'
          AND node_props.prop_value REGEXP '^[0-9]+$'   
          AND (UNIX_TIMESTAMP()-GREATEST(nodes.created, nodes.modified, nodes.updated, IFNULL(stats.last_downloaded, 0))/1000)/86400 > CAST(node_props.prop_value AS SIGNED INTEGER)+1
          {node_id_clause}
        ORDER BY nodes.node_id
        LIMIT {batch_size}
        '''
        # logging.info("Running SQL: %s", sql)
        # logging.info("Params: %s", params)
        logging.info("Executing query to fetch %d rows%s", batch_size,
                     f" from node_id {last_node_id:,d}" if last_node_id else "")
        return self.sql_query(sql, params)

    def process_batches(self, batch_size):
        last_node_id = None
        count = 0

        while True:
            rows = self.get_batch(last_node_id, batch_size)
            if len(rows) == 0:
                logging.info("No more records to process for repo %s", self.repo)
                break
            last_node_id = rows[-1].node_id
            logging.info("Got %d rows, first node_id: %s last node_id: %s", len(rows), f"{rows[0].node_id:,d}",
                         f"{last_node_id:,d}")
            yield rows
            count += len(rows)

    @staticmethod
    def get_age_days(st, stats):
        ts_now = datetime.utcnow().replace(tzinfo=timezone.utc)
        ld_ts = 0
        if stats:
            ld_ts = stats.get('lastDownloaded', 0)
        last_downloaded = datetime.utcfromtimestamp(ld_ts / 1000).replace(tzinfo=timezone.utc)
        ts = max(st.ctime, st.mtime, last_downloaded)
        age_days = (ts_now - ts).total_seconds() / (24 * 3600)
        return age_days

    def delete_artifact(self, afp, age_days, retention_days):
        assert age_days > retention_days + 1
        # logging.info("Deleting %s age_days %s retention_days %s", afp.path_in_repo, age_days, retention_days)
        try:
            afp.unlink()
        except Exception as ex:
            logging.error("Failed to delete %s %s", afp, ex)
            return "ERROR", f"Failed to delete {ex}"
        # logging.info("Deleted %s", afp.path_in_repo)
        return "DELETED", ""

    def purge_row(self, row, *, dryrun=True, verify=True):
        if row.age_days < 3:
            return "ERROR", f"Age {row.age_days:.1f} too short"

        afp = self.root.joinpath(f"{row.node_path}/{row.node_name}")
        if not verify:
            if not dryrun:
                return self.delete_artifact(afp, row.age_days, row.retention_days)
            return "DRYRUN_SUCCESS", ""

        try:
            st = afp.stat()
        except FileNotFoundError:
            return "WARNING", "NOT_FOUND"
        # print('st: ', st)

        if st.is_dir:
            return "ERROR", "IS_DIR"

        try:
            stats = self.afi.get_stats(afp)
        except Exception as ex:
            return "ERROR", f"STATS: {ex}"

        # logging.info("stats: %s", stats)
        age_days = self.get_age_days(st, stats)
        retention_days = self.get_retention_days(afp)

        if age_days > retention_days + 1:
            if not dryrun:
                return self.delete_artifact(afp, age_days, retention_days)
            else:
                return "DRYRUN_SUCCESS", "VERIFIED"
        else:
            err = "BAD_TARGET age_days=%s sql.age_days=%s retention_days=%s sql.retention_days=%s" % (
            age_days, row.age_days, retention_days, row.retention_days)
            return "ERROR", err

    @staticmethod
    def get_output_file(instance: str, repo: str, file_type: str) -> str:
        """
        file_type = [csv|log]
        """
        assert file_type in ['csv', 'log']
        # dir_prefix = rf'\\FMS-GNM-DBM-A3\RVSHARE\Artifactory' if platform.system() == "Windows" else '/infrastructure/af_cleanup'
        dir_prefix = rf'C:\temp\Artifactory' if platform.system() == "Windows" else '/infrastructure/af_cleanup'
        assert os.path.isdir(dir_prefix), f"dir_prefix {dir_prefix} not found"

        prefix = os.path.join(dir_prefix, instance, repo)
        if not os.path.isdir(prefix):
            os.makedirs(prefix)
        assert os.path.isdir(prefix)
        ts = datetime.now()
        out_file = os.path.join(prefix, f"af_cleanup_{ts:%Y-%m-%d-%H-%M}.{file_type}")
        return out_file


def log_progress(args, metrics, ts_beg):
    duration = datetime.now() - ts_beg
    rate = metrics['total_count'] / max(duration.total_seconds(), 1)
    duration = str(duration).split('.')[0]
    if args.count > 0:
        target = args.batchsize * args.count
        pct_completed = 100 * metrics['total_count'] / target
        remaining = target - metrics['total_count']
        eta = str(timedelta(seconds=remaining / rate)).split('.')[0] if rate > 0 else ""
        logging.info(
            f"{args.instance}/{args.repo}: {metrics['total_count']}/{target} = {pct_completed:.1f}% completed in {duration}, rate = {rate * 60:.1f} deletes/min. ETA = {eta}")
    else:
        logging.info(
            f"{args.instance}/{args.repo}: {metrics['total_count']} completed in {duration}, rate = {rate * 60:.1f} deletes/min.")


def multi_repo_purge(args):
    """
    Processes the list of repos found in the include list of config.json
    Each repo will output to a separate csv and log
    :param args: Command line aruguments
    :return: None
    """

    included_repolist = AFConfig(args.instance).get_repolist()

    for repo in included_repolist:
        assert repo in ['Test1', 'Test2']
        af_purger = AFRepoPurger(args.instance, repo)
        csv_file: str = AFRepoPurger.get_output_file(args.instance, repo, 'csv')
        log_file: str = AFRepoPurger.get_output_file(args.instance, repo, 'log')
        logging.basicConfig(filename=log_file, filemode='w',
                            format='%(name)s - %(levelname)s - %(message)s',
                            level=logging.INFO)

        set_logger(log_file)
        logging.info(AF_CLEANUP_VERSION+'Running with Config.json: %s', args)

        metrics = defaultdict(int)
        ts_beg = datetime.now()
        logging.info("Writing to output file: %s", csv_file)
        with open(csv_file, "w", newline='') as out_fd:
            csv_writer = csv.writer(out_fd)
            for i, row_batch in enumerate(af_purger.process_batches(batch_size=args.batchsize)):
                if i == 0:
                    csv_writer.writerow(list(row_batch[0]._fields) + ['status', "message"])
                for row in row_batch:
                    status, message = af_purger.purge_row(row, dryrun=not args.delete, verify=not args.noverify)
                    if status not in ['DELETED', 'DRYRUN_SUCCESS']:
                        logging.error(message)
                    csv_writer.writerow(list(row) + [status, message])
                    metrics['total_count'] += 1
                    metrics['deleted_count'] += (status in ['DELETED', 'DRYRUN_SUCCESS'])
                    if metrics['total_count'] % 200 == 0:
                        log_progress(args, metrics, ts_beg)

                if args.count > 0 and i >= args.count - 1:
                    break

        log_progress(args, metrics, ts_beg)
        metrics['failed_count'] = metrics['total_count'] - metrics['deleted_count']
        logging.info("Metrics: %s", dict(metrics))


def main(args):
    af_purger = AFRepoPurger(args.instance, args.repo)
    csv_file = AFRepoPurger.get_output_file(args.instance, args.repo, 'csv')
    log_file = AFRepoPurger.get_output_file(args.instance, args.repo, 'log')
    logging.basicConfig(filename=log_file, filemode='w',
                        format='%(name)s - %(levelname)s - %(message)s',
                        level=logging.INFO)

    set_logger(log_file)
    logging.info(AF_CLEANUP_VERSION + 'Running with command line args: %s', args)
    # assert args.repo in ['sfip-devops-ci-il-local', 'mountevans_sw_bsp-or-local', 'ramvarra-test']



    metrics = defaultdict(int)
    ts_beg = datetime.now()
    logging.info("Writing to output file: %s", csv_file)
    with open(csv_file, "w", newline='') as out_fd:
        csv_writer = csv.writer(out_fd)
        for i, row_batch in enumerate(af_purger.process_batches(batch_size=args.batchsize)):
            if i == 0:
                csv_writer.writerow(list(row_batch[0]._fields) + ['status', "message"])
            for row in row_batch:
                status, message = af_purger.purge_row(row, dryrun=not args.delete, verify=not args.noverify)
                if status not in ['DELETED', 'DRYRUN_SUCCESS']:
                    logging.error(message)
                csv_writer.writerow(list(row) + [status, message])
                metrics['total_count'] += 1
                metrics['deleted_count'] += (status in ['DELETED', 'DRYRUN_SUCCESS'])
                if metrics['total_count'] % 200 == 0:
                    log_progress(args, metrics, ts_beg)

            if args.count > 0 and i >= args.count - 1:
                break

    log_progress(args, metrics, ts_beg)
    metrics['failed_count'] = metrics['total_count'] - metrics['deleted_count']
    logging.info("Metrics: %s", dict(metrics))


def process_args():
    parser = argparse.ArgumentParser(
        description=AF_CLEANUP_VERSION + 'Archive artifacts based on retention.days setting. Artifacts older than '
                                         'retention.days setting will be deleted.  Age of an artifact is calcuated '
                                         'based on max(create,modified, last_downloaded) time')
    parser.add_argument('--instance', "-i", required=True,
                        help='Artifactory instance nickname - or1, is, ir, ba, ...  Required')
    # parser.add_argument('--repo', "-r", required=True, help='Name of the local repository to archive. Required.')
    repogroup = parser.add_mutually_exclusive_group(required=True)
    repogroup.add_argument('--repo', "-r")
    repogroup.add_argument('--RepoFromConfig', "-R", action="store_true")
    parser.add_argument('--delete', "-d", default=False, action="store_true",
                        help='Delete the identified objects. If not specified, runs in dry run mode. Default is '
                             'dryrun mode')
    parser.add_argument('--noverify', "-n", default=False, action="store_true",
                        help='Re verify the age and retention settings before deleting using the Artifactory API. '
                             'Default=false')
    parser.add_argument('--batchsize', "-b", type=int, default=1000,
                        help='Size of batch of records extracted from DB. Default=1000')
    parser.add_argument('--count', "-c", type=int, default=0,
                        help='Number of batches, i.e  until a the query return zero size batch. Default is unlimited')

    return (parser.parse_args())


def set_logger(log_file_name, level=logging.INFO):
    """
    Replace the existing file handler, so the output gets directed to a new file.
    :param Logger_file_name:File Name of the Handler
    :param level:
    :return:
    """
    logformatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # create file handler with the file name
    filehandler = logging.FileHandler(log_file_name, 'w')
    filehandler.setFormatter(logformatter)

    # create file handler with the file name
    consolehandler = logging.StreamHandler()
    consolehandler.setFormatter(logformatter)

    # remove all the old handlers
    root_logger = logging.getLogger()
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # add the file and console handler
    root_logger.addHandler(filehandler)
    root_logger.addHandler(consolehandler)

# ---------------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------------
if __name__ == '__main__':
    args = process_args()
    if args.RepoFromConfig is False:
        main(args)
    else:
        multi_repo_purge(args)
