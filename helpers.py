from omegaconf import OmegaConf

config_file = './config/config.yaml'

def get_db_connection()->str :
    conf = OmegaConf.load(config_file)
    return conf.DATABASE.DATABASE_URL

def get_api_key()->str :
    conf = OmegaConf.load(config_file)
    return conf.APP.API_KEY

def get_request_types()->dict:
    conf = OmegaConf.load(config_file)
    return conf.TMDB_REQUEST_TYPES
