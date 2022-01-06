import lithops

temp_from_bucket_credentials = {
    {
        "apikey": "...",
        "cos_hmac_keys": {
            "access_key_id": "...",
            "secret_access_key": "..."
        },
        "endpoints": "https://control.cloud-object-storage.cloud.ibm.com/v2/endpoints",
        "iam_apikey_description": "...",
        "iam_apikey_name": "Service credentials-1",
        "iam_role_crn": "crn:v1:bluemix:public:iam::::serviceRole:Writer",
        "iam_serviceid_crn": "...",
        "resource_instance_id": "..."
    }
}

config = {'lithops': {'backend': 'ibm_cf', 'storage': 'ibm_cos'},

          'ibm_cf': {'endpoint': 'https://eu-de.functions.cloud.ibm.com',
                     'namespace': 'Namespace-H1x',
                     'api_key': '...',
                     },
          'ibm_cos': {'storage_bucket': 'cloud-object-storage-a9-cos-standard-p24',
                      'region': 'eu-de',
                      "access_key": "...",
                      "secret_key": "..."
                      }
          }


def hello_world(name):
    return 'Hello {}!'.format(name)


if __name__ == '__main__':
    # fexec = lithops.FunctionExecutor()
    fexec = lithops.FunctionExecutor(config=config)
    fexec.call_async(hello_world, 'World')
    print(fexec.get_result())



