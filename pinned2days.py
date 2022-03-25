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

# import logutil

AF_PINNED2DAYS_VERSION = 'AF PINNED2DAYS V1.0: '

from af_cleanup import AFRepoPurger, set_logger


class AFMapPinned2Days(AFRepoPurger):

    @staticmethod
    def is_valid_pinned_str(s):
        if s is None:
            return False
        try:
            int(s)
            return True
        except ValueError:
            return False

    def set_retention_days_from_retention_pinned(self, row, *, dryrun=True):
        afp = self.root.joinpath(f"{row.node_path}/{row.node_name}")

        if row.retention_days is not None and row.retention_days != '':
            return 'DRYRUN_SKIPPED' if dryrun else 'SKIPPED', f'retention.days {row.retention_days} already exists'

        if self.is_valid_pinned_str(row.retention_pinned):
            rp = int(row.retention_pinned)
            rd = rp * 360
            if not dryrun:
                props = afp.properties
                props['retention.days'] = f'{rd}'
                afp.properties = props
                return 'UPDATED', f'retention.days set to {rd} from Pinned'
            else:
                return 'DRYRUN_UPDATED', f'retention.days set {rd} from Pinned'
        else:
            return 'DRYRUN_INVALID' if dryrun else 'INVALID', 'retention.days NOT set from Pinned'

    def get_batch(self, last_node_id, batch_size):
        params = [self.repo]
        node_id_clause = ""
        if last_node_id:
            params += [last_node_id]
            node_id_clause = " AND nodes.node_id > %s"

        sql = f'''
        SELECT 
          nodes.node_id, nodes.node_path, nodes.node_name
          , MAX(IF(node_props.prop_key = "retention.pinned", node_props.prop_value, NULL)) as retention_pinned
          , MAX(IF(node_props.prop_key = "retention.days", node_props.prop_value, NULL)) as retention_days
        FROM nodes 
          LEFT JOIN node_props ON (nodes.node_id = node_props.node_id)
          LEFT JOIN stats ON (nodes.node_id = stats.node_id)
        WHERE 
          nodes.repo = %s
          AND nodes.node_type = 1		
          AND (node_props.prop_key = 'retention.days' OR node_props.prop_key = 'retention.pinned')
          AND node_props.prop_value REGEXP '^[0-9]+$'   
          {node_id_clause}
        GROUP BY nodes.node_id
        LIMIT {batch_size}
        '''
        # logging.info("Running SQL: %s", sql)
        # logging.info("Params: %s", params)
        logging.info("Executing query to fetch %d rows%s", batch_size,
                     f" from node_id {last_node_id:,d}" if last_node_id else "")
        return self.sql_query(sql, params)


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
            f"{args.instance}/{args.repo}: {metrics['total_count']}/{target} = {pct_completed:.1f}% completed in {duration}, rate = {rate * 60:.1f} updates/min. ETA = {eta}")
    else:
        logging.info(
            f"{args.instance}/{args.repo}: {metrics['total_count']} completed in {duration}, rate = {rate * 60:.1f} updates/min.")


def pinned2days(args):
    af_purger = AFMapPinned2Days(args.instance, args.repo)
    csv_file = AFMapPinned2Days.get_output_file(args.instance, args.repo, 'csv')
    log_file = AFMapPinned2Days.get_output_file(args.instance, args.repo, 'log')
    logging.basicConfig(filename=log_file, filemode='w',
                        format='%(name)s - %(levelname)s - %(message)s',
                        level=logging.INFO)

    set_logger(log_file)
    logging.info(AF_PINNED2DAYS_VERSION + 'Running with command line args: %s', args)
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
                status, message = af_purger.set_retention_days_from_retention_pinned(row, dryrun=not args.apply)
                if status not in ['UPDATED', 'DRYRUN_UPDATED', 'DRYRUN_SKIPPED', 'SKIPPED']:
                    logging.error(message)
                csv_writer.writerow(list(row) + [status, message])
                metrics['total_count'] += 1
                metrics['updated_count'] += (status in ['UPDATED', 'DRYRUN_UPDATED'])
                metrics['skipped_count'] += (status in ['SKIPPED', 'DRYRUN_SKIPPED'])
                if metrics['total_count'] % 200 == 0:
                    log_progress(args, metrics, ts_beg)

            if args.count > 0 and i >= args.count - 1:
                break

    log_progress(args, metrics, ts_beg)
    metrics['failed_count'] = metrics['total_count'] - metrics['updated_count'] - metrics['skipped_count']
    logging.info("Metrics: %s", dict(metrics))


def process_args():
    parser = argparse.ArgumentParser(
        description=AF_PINNED2DAYS_VERSION + 'Update retention.days attribute based on the value of retention.pinned')
    parser.add_argument('--instance', "-i", required=True,
                        help='Artifactory instance nickname - or1, is, ir, ba, ...  Required')
    parser.add_argument('--repo', "-r", required=True, help='Name of the local repository to archive. Required.')
    parser.add_argument('--apply', "-a", default=False, action="store_true",
                        help='Apply the identified changes. If not specified, runs in dry run mode. Default is '
                             'dryrun mode')
    parser.add_argument('--batchsize', "-b", type=int, default=1000,
                        help='Size of batch of records extracted from DB. Default=1000')
    parser.add_argument('--count', "-c", type=int, default=0,
                        help='Number of batches, i.e  until a the query return zero size batch. Default is unlimited')

    return (parser.parse_args())


# ---------------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------------
if __name__ == '__main__':
    args = process_args()
    pinned2days(args)
