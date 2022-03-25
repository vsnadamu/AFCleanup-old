# Aging Artifacts in Artifactory
import json
import os
# import io
# import sys
from datetime import datetime, timezone, timedelta
# import requests
import tempfile
import time

from artifactory import ArtifactoryPath

from af_cleanup import AFRepoPurger

def af_create_tmpdiskfile():
    # create a file and upload it
    tmpdir = tempfile.mkdtemp()
    fpath = os.path.join(tmpdir, 'testcase.txt')
    with open(fpath, "a") as fp:
        fp.write('Hello World Testcase!')
    return fpath


def af_delete_test_folder(afpr: AFRepoPurger, nodepath):
    mysession = afpr.afi._af.session
    path: ArtifactoryPath = afpr.afi._af
    #assert str(path) in ['https://fm-next-af.devtools.intel.com/artifactory']
    assert str(path) in ['https://af01p-fm-app06.devtools.intel.com/artifactory']

    # delete the current TEST directory
    mypath = path.joinpath(f'/{afpr.repo}', nodepath)
    if mypath.exists():
        #print('Test Cases Folder ' + str(mypath) + ' exists from before, it will be deleted')
        resp = mysession.delete(mypath)
        #print(resp.text)
    else:
        print(f'Test Cases Folder {nodepath} does not exists and No delete required')


def af_download_artifact(afpr: AFRepoPurger, nodepath, nodename):
    mysession = afpr.afi._af.session
    path: ArtifactoryPath = afpr.afi._af

    # delete the current TEST directory
    mypath = path.joinpath(f'/{afpr.repo}', nodepath, nodename)

    result = mysession.get(str(mypath))

    if result.status_code == 200:
        return True
    else:
        print(json.loads(result.content)['error'])
        return False


# create an artifact, set its retention.days and create a download record
def af_create_test_artifacts(afpr: AFRepoPurger, disk_file, node_path, node_name, retention_days, retention_pinned, purge_candidate):
    path: ArtifactoryPath = afpr.afi._af
    mypath = path.joinpath(f'/{afpr.repo}', node_path, node_name)
    #print('Deploying: ' + str(mypath))
    mypath.deploy_file(disk_file)
    props = mypath.properties
    props['purge_candidate'] = f'{"Y" if purge_candidate is None else purge_candidate}'
    mypath.properties = props
    if retention_days is not None:
        props = mypath.properties
        props['retention.days'] = f'{retention_days}'
        mypath.properties = props
    if retention_pinned is not None:
        props = mypath.properties
        props['retention.pinned'] = f'{retention_pinned}'
        mypath.properties = props


# This method directly updates the database records for a given artifact by backdating the dates
# deprecated
#todo - skip update of the value is null
def af_age_artifacts_days_from_today(afpr: AFRepoPurger, nodepath, nodename, created_days, modified_days, updated_days,
                                     download_days):
    #print("Aging the artifact...")
    sql = 'UPDATE nodes \nset '
    if created_days != "":
        sql = sql + f' created  = (UNIX_TIMESTAMP() - ({created_days} * 86400)) * 1000\n'
        comma = '   ,'
    else:
        comma = ''
    if modified_days != "":
        sql = sql + comma + f' modified = (UNIX_TIMESTAMP() - ({modified_days} * 86400)) * 1000\n'
        comma = '   ,'
    else:
        comma = ''
    if updated_days != "":
        sql = sql + comma + f' updated  = (UNIX_TIMESTAMP() - ({updated_days} * 86400)) * 1000 \n'
    sql = sql + f'''WHERE nodes.repo = "{afpr.repo}" 
    AND nodes.node_path = "{nodepath}"
    AND nodes.node_name = "{nodename}"
    '''
    # print(sql)
    rows = afpr.sql_query(sql, None)

    # get the node_id from nodes
    sql = f'''
    SELECT nodes.node_id
    FROM nodes
    WHERE nodes.repo    = "{afpr.repo}"
    AND nodes.node_path = "{nodepath}"
    AND nodes.node_name = "{nodename}"      
    '''
    rows = afpr.sql_query(sql, None)
    # print(sql)
    # insert download record into stats
    if download_days != "":
        sql = f'''
        INSERT INTO stats (node_id, download_count,  last_downloaded, last_downloaded_by)
        VALUES ('{rows[0].node_id}', '1', (UNIX_TIMESTAMP() - {download_days} * 86400)*1000, 'sys_artifact') 
        '''
        rows = afpr.sql_query(sql, None)
        #print(sql)


# This method directly updates the database records  for a given artifact to set its age
def af_age_artifacts(afpr: AFRepoPurger, nodepath, nodename, created_date, modified_date, updated_date, download_date):
    # update the timestamps for the deployed file
    #assert str(afpr.afi._af) in ['https://fm-next-af.devtools.intel.com/artifactory']
    assert str(afpr.afi._af) in ['https://af01p-fm-app06.devtools.intel.com/artifactory']

    sql = f'''
        UPDATE nodes 
        set created  = UNIX_TIMESTAMP("{created_date}")*1000, 
            modified = UNIX_TIMESTAMP("{modified_date}")*1000,
            updated = UNIX_TIMESTAMP("{updated_date}")*1000
        WHERE nodes.repo = "{afpr.repo}"
        AND nodes.node_path = "{nodepath}"
        AND nodes.node_name = "{nodename}"          
    '''
    rows = afpr.sql_query(sql, None)

    # get the node_id from nodes
    sql = f'''
        SELECT nodes.node_id
        FROM nodes
        WHERE nodes.repo = "{afpr.repo}"
        AND nodes.node_path = "{nodepath}"
        AND nodes.node_name = "{nodename}"      
    '''
    rows = afpr.sql_query(sql, None)

    # insert download record into stats
    sql = f'''
        INSERT INTO stats (node_id, download_count,  last_downloaded, last_downloaded_by)
        VALUES ('{rows[0].node_id}', '1', UNIX_TIMESTAMP("{download_date}")*1000, 'vsnadamu') 
    '''
    rows = afpr.sql_query(sql, None)

#todo - get all folders from config
def af_get_file_list(afpr: AFRepoPurger, node_path_list):
    path: ArtifactoryPath = afpr.afi._af

    npfilter = resultn = resulty = []
    for np in node_path_list:
        npfilter = npfilter + [{"path": f"{np}"}]

    # findstr = {"repo": "Test1",
    #             "$or": [{"path": "Folder01"}, {"path": "Folder02"}]
    #             }

    findstr = {"repo": f"{afpr.repo}", "$or": npfilter}
    artifacts = path.aql("items.find", findstr, ".include", ["repo", "path", "name", "property"])
    for art in artifacts:
        props = art['properties']
        for prop in props:
            if prop['key'] == 'purge_candidate':
                if prop['value'] == 'Y':
                    resulty = resulty + [[art['repo'], art['path'], art['name'], prop['value']]]
                if prop['value'] == 'N':
                    resultn = resultn + [[art['repo'], art['path'], art['name'], prop['value']]]

    print('Yes List: ', resulty)
    print('No  List: ', resultn)
    return resulty, resultn


#todo - get all folders from config
def af_get_file_list2(afpr: AFRepoPurger, node_path_list):
    path: ArtifactoryPath = afpr.afi._af

    npfilter = resultn = resulty = []
    for np in node_path_list:
        npfilter = npfilter + [{"path": f"{np}"}]

    findstr = {"repo": f"{afpr.repo}", "$or": npfilter}
    artifacts = path.aql("items.find", findstr, ".include", ["repo", "path", "name", "property"])
    return artifacts


def purge_artifacts(afp, batch_size, dryrun, verify):
    #assert str(afp.afi._af) in ['https://fm-next-af.devtools.intel.com/artifactory']
    assert str(afp.afi._af) in ['https://af01p-fm-app06.devtools.intel.com/artifactory']

    purged = False
    for i, row_batch in enumerate(afp.process_batches(batch_size=batch_size)):
        for row in row_batch:
            status, msg = afp.purge_row(row, dryrun=dryrun, verify=verify)
            purged = True
            print(row, msg, status)
    return purged

def create_and_age_test_artifacts(afp, disk_file, node_path, node_name, created, modified, updated, downloaded,
                                  retention_days, retention_pinned, purge_candidate):
    af_create_test_artifacts(afp, disk_file, node_path, node_name, retention_days, retention_pinned, purge_candidate)
    af_age_artifacts_days_from_today(afp, node_path, node_name, created, modified, updated, downloaded)


def create_test_config(folder):
    userdata = []

    for i in range(10):
        row = [{
            "node_path": folder,
            "node_name": f"case{i}.txt",
            "created": "2022-01-26",
            "modified": "2022-01-27",
            "updated": "2022-01-28",
            "downloaded": "",
            "retention_days": "10",
            "retention_pinned": "3"
        }]
        userdata = userdata + row
    return userdata


if __name__ == '__main__':

    # afp = AFRepoPurger(instance='is', repo='cvg-mbly-local')
    #
    # node_path_list = ['bootrom_eq6_val/cut1/manual']
    # afacts = af_get_file_list2(afp, node_path_list)
    #
    # for af in afacts:
    #     print(af)
    # exit(0)


    test_config_file = 'testconfig.json'
    assert os.path.exists(test_config_file), f"Config file {test_config_file} not found"
    test_config = json.load(open(test_config_file)).get('TestConfig1')
    instance = test_config.get('instance')
    repo = test_config.get('repo')
    print('Working on test repository: ', repo, ' on  instance: ', instance)
    # init purger
    afp = AFRepoPurger(instance=instance, repo=repo)
    # create a temporary file for uploads
    disk_file = af_create_tmpdiskfile()
    # loop through and cleanup folders created by previous run
    unique_node_paths = {artifact.get('node_path') for artifact in test_config.get('artifacts')}
    # Query using AQL
    print('Cleaning up Test cases folders...', unique_node_paths)
    for folder in unique_node_paths:
        af_delete_test_folder(afp, folder)
    # loop through and upload the file and set its retention days and age them
    print("Creating and Aging the artifacts...")

    for artifact in test_config.get('artifacts'):
        node_path = artifact.get('node_path')
        node_name = artifact.get('node_name')
        created = artifact.get('created')
        modified = artifact.get('modified')
        updated = artifact.get('updated')
        downloaded = artifact.get('downloaded')
        retention_days = artifact.get('retention_days')
        retention_pinned = artifact.get('retention_pinned')
        purge_candidate= artifact.get('purge_candidate')
        print(instance, repo, node_path, node_name, created, modified, updated, downloaded, retention_days, retention_pinned, purge_candidate)
        create_and_age_test_artifacts(afp, disk_file, node_path, node_name, created, modified, updated, downloaded,
                                      retention_days, retention_pinned, purge_candidate)
    fl1_y, fl1_n = af_get_file_list(afp, unique_node_paths)
    print("Setup is complete...")
    # Purge as delete run
    print("Purge/delete Artifacts ....")
    assert (purge_artifacts(afp, batch_size=5000, dryrun=False, verify=True) is True)
    # Purge as dryrun to verify that there is no more
    print("Purge/dryrun...")
    assert (purge_artifacts(afp, batch_size=5000, dryrun=True, verify=True) is False)
    # List files to be purged using AQL query
    fl2_y, fl2_n = af_get_file_list(afp, unique_node_paths)
    assert (fl1_n == fl2_n)
    assert (fl1_y != fl2_y and fl2_y == [])
