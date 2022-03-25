import os
import platform
import json

class AFConfig:
  def init_keyring(self):
    self.kr_system = "ARTIFACTORY"
    if platform.system() == 'Linux':
      from keyrings.cryptfile.cryptfile import CryptFileKeyring
      self.kr = CryptFileKeyring()
      self.kr.keyring_key = self.kr_system
    else:
      import keyring
      self.kr = keyring

  def __init__(self, instance, config_file='config.json'):
    self.init_keyring()
    if config_file is None:
      return

    assert os.path.exists(config_file), f"Config file {config_file} not found"
    config = json.load(open(config_file))
    self.config = config.get(instance)

    if not self.config:
      raise Exception(f"AF Instance {instance} not defined in config file {config_file}")
    self.config['auth'] = [self.resolve_password(var) for var in self.config['auth']]
    if db := self.config.get('database'):
      db['auth'] = [self.resolve_password(var) for var in db['auth']]

  def get_repolist(self):
    if self.config['included-repos'] is not []:
      return self.config['included-repos']
    else:
      #todo need to get the whole repolist and remove
      return self.config['excluded-repos']

  def get_db_config(self, role='PRIMARY'):
    assert role in ['PRIMARY', 'STANDBY']
    db = self.config['database']
    return dict(
      user=db['auth'][0],
      password=db['auth'][1],
      database=db['database'],
      host=db['primary_host'] if role == 'PRIMARY' else db['standby_host'],
      port=db['port'],
      # ssl enable if DBaaS
      ssl_disabled=False if db['ssl_disabled'] is False else True
    )

  def resolve_password(self, value):
    '''
    Process ENV, KEYRING prefixes
    '''
    if ':' not in value:
        return value
    prefix, var = value.split(':')
    if prefix == 'ENV':
        assert var in os.environ, f"Envioronmnet variable {var} not defined"
        return os.getenv(var)
    if prefix == 'KEYRING':
      return self.keyring_lookup(var)

    raise Exception(f"Unrecognized scheme {prefix} for var {var}")

  def keyring_setup(self, name, pwd):
    '''
    e.g. Usage:
      from AFConfig import AFConfig
      afc = AFConfig(None, config_file=None)
      afc.keyring_setup('SYS_ARTIFACT', 'xxxx')
      afc.keyring_setup('DB_PASSWORD', 'xxxx')
    '''
    self.kr.set_password(self.kr_system, name, pwd)
    print(f"Setup {name} password in keyring")

  def keyring_lookup(self, name):
    try:
      pwd = self.kr.get_password(self.kr_system, name)
    except Exception as ex:
      raise Exception(f"Failed to get password from keyring for system {self.kr_system} name {name}: {ex}")

    if pwd is None:
      msg = (f"Password not defined in keyring for system {self.kr_system} name {name} - " 
            f"use af_config.keyring_setup('{name}', 'pwd') to define it")
      raise Exception(msg)

    return pwd

