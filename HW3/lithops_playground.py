import lithops

temp_from_bucket_credentials = {
    {
        "apikey": "VqT59Ip4_SNWh9HYbN-cGHbbc1Yq2MRTnIcOscuW06lL",
        "cos_hmac_keys": {
            "access_key_id": "934602118fbf401e9ed3ed9a4c3d3d30",
            "secret_access_key": "a064c5b8688dfb4bb5c07e9fb4549dfc4bd33c19bdb48780"
        },
        "endpoints": "https://control.cloud-object-storage.cloud.ibm.com/v2/endpoints",
        "iam_apikey_description": "Auto-generated for key crn:v1:bluemix:public:cloud-object-storage:global:a/743281a5dfd447168259c9e2c5192322:d40d09c4-1f3b-4ac3-9484-20bd3c030ce9:resource-key:93460211-8fbf-401e-9ed3-ed9a4c3d3d30",
        "iam_apikey_name": "Service credentials-1",
        "iam_role_crn": "crn:v1:bluemix:public:iam::::serviceRole:Writer",
        "iam_serviceid_crn": "crn:v1:bluemix:public:iam-identity::a/743281a5dfd447168259c9e2c5192322::serviceid:ServiceId-00211bf8-8bda-4808-a457-84ab15611ebf",
        "resource_instance_id": "crn:v1:bluemix:public:cloud-object-storage:global:a/743281a5dfd447168259c9e2c5192322:d40d09c4-1f3b-4ac3-9484-20bd3c030ce9::"
    }
}

config = {'lithops': {'backend': 'ibm_cf', 'storage': 'ibm_cos'},

          'ibm_cf': {'endpoint': 'https://eu-de.functions.cloud.ibm.com',
                     'namespace': 'Namespace-H1x',
                     'api_key': 'ApiKey-c4feadee-115e-4b91-a38a-7926d3cdf168',
                     },
          'ibm_cos': {'storage_bucket': 'cloud-object-storage-a9-cos-standard-p24',
                      'region': 'eu-de',
                      "access_key": "934602118fbf401e9ed3ed9a4c3d3d30",
                      "secret_key": "a064c5b8688dfb4bb5c07e9fb4549dfc4bd33c19bdb48780"
                      }
          }


def hello_world(name):
    return 'Hello {}!'.format(name)


if __name__ == '__main__':
    # fexec = lithops.FunctionExecutor()
    fexec = lithops.FunctionExecutor(config=config)
    fexec.call_async(hello_world, 'World')
    print(fexec.get_result())



