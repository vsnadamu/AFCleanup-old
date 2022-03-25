# pylint: disable=no-member
import os
import re
import logging
from datetime import datetime, timezone
import json
import sys
import requests
from pprint import pprint, pformat
import platform

from artifactory import ArtifactoryPath
import logutil


import urllib3

# urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

'''
Windows monkey_patch to enable tcp_keepalive
'''
if platform.system() == "Windows":
    import socket
    original_connect_https = urllib3.connection.HTTPSConnection.connect

    def tcp_keepalive_patch_to_connect_https(_self):        
        original_connect_https(_self)
        #_self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        logging.debug("Setting keep alive settings to socket: %s", _self.sock)
        # start keep alive after 120 secs, keep sending every 30secs 
        _self.sock.ioctl(socket.SIO_KEEPALIVE_VALS, (1, 120*1000, 30*1000))
    
    urllib3.connection.HTTPSConnection.connect = tcp_keepalive_patch_to_connect_https

'''
END monkey_patch
'''

class AFInstance:
    
    @staticmethod
    def keyring_setup(system, name, pwd):
        if platform.system() == 'Linux':
            from keyrings.cryptfile.cryptfile import CryptFileKeyring
            kr = CryptFileKeyring()
            kr.keyring_key = system
        else:
            kr = keyring
        kr.set_password(system, name, pwd)

    @staticmethod
    def keyring_lookup(system, name):
        try:
            import keyring
        except Exception as ex:
            logging.error("Unable to import keyring module to process var: %s - %s", ex)
            raise
            
        if platform.system() == 'Linux':
            try:
                from keyrings.cryptfile.cryptfile import CryptFileKeyring
            except Exception as ex:
                logging.error("Required module keyrings.cryptfile.cryptfile not installed - use rv pybuild")
                raise
            kr = CryptFileKeyring()
            kr.keyring_key = system
        else:
            kr = keyring
        try:
            pwd = kr.get_password(system, name)
        except Exception as ex:
            logging.error("Failed to get password from keyring for system %s name %s: %s", system, name, ex)
            raise
        if pwd is None:
            msg = "Password not defined in keyring for system %s name %s - use AFInstance.keyring_setup('ARTIFACTORY', %s, 'pwd') to define it" % (system, name, name)
            logging.error(msg)
            raise Exception(msg)
            
        return pwd

    @staticmethod            
    def resolve_password(system, value):
        '''
        Process ENV, KEYRING prefixes
        '''
        if ':' not in value:
            return value
        prefix, var = value.split(':')
        if prefix == 'ENV':
            assert var in os.environ, "%s config envioronmnet variable %s not defined" % (system, var)
            return os.getenv(var)
        
        if prefix == 'KEYRING':
            return AFInstance.keyring_lookup(system, var)

        logging.error("Unrecognized scehme %s for var %s", prefix, var)
        raise Exception("Unrecognized scehme %s for var %s" % (prefix, var))

    def __init__(self, config_file, instance):        
        assert os.path.exists(config_file)
        config = json.load(open(config_file))
        assert instance in config
        assert config[instance] and config[instance]['uri'] and config[instance]['auth']
        auth_user, auth_password = [self.resolve_password("ARTIFACTORY", var) for var in config[instance]['auth']]        
        self._af = ArtifactoryPath(config[instance]['uri'], auth=(auth_user, auth_password))

    def _add_keep_alive(self):
        from requests_toolbelt.adapters.socket_options import TCPKeepAliveAdapter
        # enable keep alives
        # after 180 secs of TCP Channel is idle, start keepalive ACKs. 10 ACks with 10sec interval
        keep_alive = TCPKeepAliveAdapter(idle=180, count=10, interval=10)
        schemes = list(self._af.session.adapters.keys())
        for scheme in schemes:
            logging.info("Adding keepalive to %s", scheme)
            self._af.session.mount(scheme, keep_alive)

    @staticmethod
    def iso_to_datetime(iso_string):
        '''
        iso_string: str of format: '2015-07-22T18:30:09.542-07:00'
        returns: datetime.datetime object
        '''
        return datetime.fromisoformat(iso_string).astimezone(timezone.utc)

    def aql_query(self, query_body, props=None, verbose=False):
        '''
        run aql - either dict or list of dicts
        '''
        if isinstance(query_body, list) or isinstance(query_body, tuple):
            # convert to dict
            query = {}
            for d in query_body:
                assert isinstance(d, dict) and len(d.keys()) == 1
                for k, v in d.items():
                    query[k] = v
        else:
            assert isinstance(query_body, dict)
            query = query_body        
        
        prop_args = [".include", props] if props else []
        if verbose:
            logging.info("Running AQL: %s", pformat(query))
        try:
            results = self._af.aql("items.find", query, *prop_args)
        except Exception as ex:
            logging.error("AQL Query Failed: %s", ex)
            raise
        
        if verbose:
            logging.info("AQL got %d results", len(results))
        return results

    @staticmethod
    def _flatten_item(r):
        # move up sub props to top level
        stats = r.pop('stats', [])
        for stat in stats:
            r.update(stat)
        props = r.pop('properties', [])
        for prop in props:
            r['@'+prop['key']] = prop['value']
    
    @staticmethod
    def get_parts(path, num_parts=3):
        parts = path.lstrip('/').split('/', num_parts)[:num_parts]
        return {'p'+str(n+1): '/'.join(parts[:n+1]) for n in range(num_parts)}
    
    def get_artifact_infos(self, repo, path_pattern):
        query = {"repo": repo}
        if path_pattern:
            query["path"] = {"$match": path_pattern}

        props = ["stat", "property", "artifact.module.build"]  
        logging.info("Submittting AQL Query: %s", query)
        results = self._af.aql("items.find",  query, ".include", props)
        logging.info("Post processing results %d", len(results))
        dt_cols = ['created', 'modified', 'updated', 'downloaded']

        for r in results:
            self._flatten_item(r)
            r.update(self.get_parts(r['path']))
            # convert dates to UTC ISO
            for col in dt_cols:
                if col in r:
                    r[col] = datetime.fromisoformat(r[col]).astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        return results

    def get_repos(self):
        url = str(self._af.joinpath("/api/repositories"))
        r = self._af.session.get(url)
        assert r.status_code == 200
        repos = r.json()
        return repos
       
    def download_infos_p1(self, repo, p1, out_dir):
        ts = datetime.utcnow().isoformat() + 'Z'
        
        out_dir = os.path.join(out_dir, repo)
        if not os.path.isdir(out_dir):
            os.mkdir(out_dir)        
        out_file = os.path.join(out_dir, p1 + '.json')
        if os.path.exists(out_file) and os.path.getsize(out_file) > 0:
            logging.info("Skipping %s/%s", repo, p1)
            return 
        
        logging.info("Fetching %s/%s -> %s", repo, p1, out_file)
        infos = self.get_artifact_infos(repo, p1+'*')
        logging.info("Found %d artifacts in repo %s/%s", len(infos), repo, p1)

        with open(out_file, "w") as fd:
            for info in infos:
                info['ts'] = ts
                json.dump(info, fd)
                fd.write('\n')
        logging.info("Wrote to %s", out_file)

    def download_infos(self, repo, out_dir):
        p1_list = [e.name for e in self._af.joinpath(repo) if e.is_dir()]
        for p1 in p1_list:
            logging.info("Processing %s/%s", repo, p1)
            self.download_infos_p1(repo, p1, out_dir)

    def get_permission_targets(self):
        url = str(self._af) + '/api/security/permissions'
        r = self._af.session.get(url)
        r.raise_for_status()
        return r.json()
    
    def get_permission_target_details(self, target):
        url = str(self._af) + '/api/security/permissions/' + target
        r = self._af.session.get(url)
        r.raise_for_status()
        return r.json()
    
    def find_permssions_for_repo(self, repo_key):
        pts = self.get_permission_targets()
        plist = []
        for pt in pts:
            r = self._af.session.get(pt['uri'])
            r.raise_for_status()
            p = r.json()
            if repo_key in p['repositories']:
                plist.append(p)
        return plist
    
    def get_group_details(self, group_name):
        url = str(self._af) + '/api/security/groups/' + group_name
        params = dict(includeUsers='true')
        r = self._af.session.get(url, params=params)
        r.raise_for_status()
        gi = r.json()
        return gi

    def joinpath(self, repo, paths=[]):
        assert repo
        if isinstance(paths, list) or isinstance(paths, tuple):
            p = paths
        else:
            assert isinstance(paths, str)
            p = [paths]
        return self._af.joinpath(repo, *p)

    def generic_item_iter(self, repo_path, predicate):
        '''
        Depth first tree walk of repo_path. Return items that satisfy predicate.
        Predicate must be callable that takes (afpath, stat_info, depth) and return
        a tuple (bool, data)  bool: True/False indicating if it should be selected.
                              data: dict that is returned back to caller                              
        '''
        def _generic_item_helper(afp):
            for child_item in afp:
                try:
                    ci_stat = child_item.stat()
                except FileNotFoundError:
                    # file/folder might have been deleted while we are processing
                    continue
                if ci_stat.is_dir and len(ci_stat.children):
                    yield from _generic_item_helper(child_item)
                    # refresh ci_stat (to account for any changes done by the yield callback)
                    try:
                        ci_stat = child_item.stat()
                    except FileNotFoundError:
                        # file/folder might have been deleted while we are processing
                        continue
                pred_status, data = predicate(child_item, ci_stat)
                if pred_status:
                    yield child_item, data
                    
        afp = repo_path if isinstance(repo_path, ArtifactoryPath) else self._af.joinpath(repo_path)
        yield from _generic_item_helper(afp)
                                
    def file_item_iter_deprecated(self, repo_path, older_than_days):
        '''
        return file artifacts that are older than age_days
          age: now - max(created_time, modified_time, last_downloaded_time)
        '''
        assert older_than_days > 0
        now = datetime.now(timezone.utc)
        def predicate(afp, st):
            if st.is_dir:
                return False, None
            mdays = (now - st.mtime).total_seconds()/(24*3600) if st.mtime else 1e7
            cdays = (now - st.ctime).total_seconds()/(24*3600) if st.ctime else 1e7
            stats = self.get_stats(afp)
            ldl_ms = stats['lastDownloaded']
            if ldl_ms:
                ldl_days =  (now - datetime.fromtimestamp(ldl_ms/1000, timezone.utc)).total_seconds()/(24*3600)
            else:
                ldl_days = 1e7
            age_days = min(mdays, cdays, ldl_days)
            return age_days > older_than_days, {'age_days': age_days, 'file_size': st.size}
        
        yield from self.generic_item_iter(repo_path, predicate)
    
    def empty_dir_iter(self, repo_path, older_than_days, min_depth):
        '''
        min_depth=minimum depth of folders (not including repo name) for selection
           <repo_name>/p1 = Depth 1. Will be seleted if min_depth<= 1
           <repo_name>/p1/p2 = Depth 2. Will be selected if min_depth<= 2

        return file artifacts that are older than age_days
          age: now - max(created_time, modified_time)
          returns: (True/False for predicate, dict(age_days=<>))     
        '''
        now = datetime.now(timezone.utc)
        def predicate(afp, st):    
            if not st.is_dir:
                return False, None     
            # afp.path_in_repo = /p1/p2....   
            depth = len(afp.path_in_repo.split('/'))-1
            assert depth >= 0, f"Bad depth {depth} for afp: {afp.repo}/{afp.path_in_repo}"
            if depth >= min_depth and len(st.children) == 0:
                mdays = (now - st.mtime).total_seconds()/(24*3600) if st.mtime else 1e7
                cdays = (now - st.ctime).total_seconds()/(24*3600) if st.ctime else 1e7
                age_days = min(mdays, cdays)
                return age_days > older_than_days, {'age_days': age_days, 'depth': depth}
            return False, None
        
        yield from self.generic_item_iter(repo_path, predicate)
    
    def get_stats(self, afp):
        """
        Get artifact stats and return them as a dictionary.
        """
        assert isinstance(afp, ArtifactoryPath)
        url = "/".join(
            [
                afp.drive.rstrip("/"),
                "api/storage",
                str(afp.relative_to(afp.drive)).strip("/"),
            ]
        )

        params = "stats"
        r = self._af.session.get(url, params=params)
        r.raise_for_status()
        return r.json()
           
    def create_dir(self, afp):
        assert afp.parts[0] == str(self._af)
        #logging.info("Creating dir: %s", str(afp))
        r = self._af.session.put(str(afp) + '/')
        r.raise_for_status()        
        #j = r.json()
        #logging.info("Resp: %s", j)
        
    def _delete(self, afp):
        assert afp.parts[0] == str(self._af)
        r = self._af.session.delete(str(afp))
        r.raise_for_status()

    def delete_empty_dir(self, afp):
        st = afp.stat()
        assert st.is_dir and len(st.children) == 0, f"Path {afp} is not dir or empty_dir"
        self._delete(afp)

    def search_checksum(self, **checksum_params):
        '''
        look up artifact by checksum. checksum_params can be one or more of 
            sha1=<sha1> sha256=<sha256> md5=<md5>
        Ref: https://www.jfrog.com/confluence/display/JFROG/Artifactory+REST+API#ArtifactoryRESTAPI-ChecksumSearch
        '''
        url = str(self._af.joinpath("/api/search/checksum"))
        assert 'sha1' in checksum_params or 'sha256' in checksum_params or 'md5' in checksum_params
        r = self._af.session.get(url, params=checksum_params)
        r.raise_for_status()
        results = r.json()
        assert 'results' in results
        return [r['uri'] for r in results['results']]
    
    def storage_check_by_url(self, url):
        '''
        Check if artifact url is present.
        The url format can be: 
            https://ubit-artifactory-or.intel.com/artifactory/api/storage/Jimmy-test-local/TestDeploy/AF-PNG.txt
            Jimmy-test-local/TestDeploy/AF-PNG.txt        
        '''
        if url.startswith('https://'):
            assert '/artifactory/api/storage/' in url
            url = "/".join(url.split('/')[6:])
            assert url
        path = self._af.joinpath(url)
        try:
            fd = path.open()
            fd.close()
        except:
            return None
        else:
            return path
    
    def storage_check_by_checksum(self, **checksum_params):
        '''
        lookup an artifact by a checksum and check if its on storage.
        Refer to search_checksum for params
        '''
        urls = self.search_checksum(**checksum_params)
        finds = []
        for url in urls:
            if self.storage_check_by_url(url):
                finds.append(url)
        return finds

    def repo_stats(self, repo_path):
        def _merge_stats(rs, sub_rs):
            for k, v in sub_rs.items():
                if k.startswith('min'):
                    rs[k] = min(rs[k], v)
                elif  k.startswith('max'):
                    rs[k] = max(rs[k], v)
                else:
                    rs[k] += v
                
        def _repo_stats_helper(afp):
            repo_stats = dict(dir_count=0, file_count=0, empty_dir_count=0, file_size=0, max_file_size=0, 
                               min_ts=datetime(3000, 1, 1,tzinfo=timezone.utc), max_ts=datetime(1970, 1, 1,tzinfo=timezone.utc))
            is_empty = True
            for child_item in afp:
                is_empty = False
                if child_item.is_dir():
                    repo_stats['dir_count'] += 1
                    sub_repo_stats = _repo_stats_helper(child_item)
                    try:
                        _merge_stats(repo_stats, sub_repo_stats)
                    except:
                        print('Child:', child_item)
                        print('Sub:', sub_repo_stats)
                        print('Repo:', repo_stats)
                        raise
                else:                    
                    file_stats = child_item.stat()                    
                    file_ts = max(file_stats.ctime, file_stats.mtime)
                    try:
                        _merge_stats(repo_stats, {'min_ts': file_ts, 'max_ts': file_ts, 
                                             'max_file_size': file_stats.size,
                                            'file_size': file_stats.size, 'file_count': 1})
                    except:
                        print('Child:', child_item)
                        print('Stats: ', file_stats)
                        raise
                    #print(file_stats)
            
            repo_stats['empty_dir_count'] += is_empty
            return repo_stats

        afp = repo_path
        if not isinstance(repo_path, ArtifactoryPath):
            afp = self._af.joinpath(repo_path)
        return _repo_stats_helper(afp)    

    def get_propeties(self, afp, join_char='|'):
        assert isinstance(afp, ArtifactoryPath)
        props = afp.properties
        props = {k: join_char.join(v) if isinstance(v, list) else v for k, v in props.items()}
        return props

    def get_propeties_old(self, afp, join_char='|'):
        if not isinstance(afp, ArtifactoryPath):
            afp = self._af.joinpath(afp)

        # dohq-artifactory behavior changed in new version. path_in_repo does not contain repo name prefix
        assert not afp.path_in_repo.startswith(f"/{afp.repo}")        
        url = f"{self._af}/api/storage/{afp.repo}/{afp.path_in_repo}"
        url_v1 = "/".join(
            [
                afp.drive.rstrip("/"),
                "api/storage",
                str(afp.relative_to(afp.drive)).strip("/"),
            ]
        )
        params = {'properties': 'retention.days,retention.pinned,build.name,build.number'}
        r = self._af.session.get(url, params=params)
        r.raise_for_status()
        results = r.json()       
        props = results.get('properties', {})
        props = {k: join_char.join(v) if isinstance(v, list) else v  for k, v in props.items()}
        return props
    
    def get_folder_summary(self, folder: ArtifactoryPath):
        repo, path = folder.parts[1], "/".join(folder.parts[2:])
        assert isinstance(folder, ArtifactoryPath)
        q = {        
            "type": "file",
            "repo": repo,
            "path": {"$match": path + "/**"},
        }
        results = self.aql_query(q)        
        d = {
            'repo': repo,
            'path': path,
            'size': sum(f['size'] for f in results),
            'count': len(results),
        }
        return d

