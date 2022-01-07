import lithops

# temp_from_bucket_credentials = {
#     {
#         "apikey": "...",
#         "cos_hmac_keys": {
#             "access_key_id": "...",
#             "secret_access_key": "..."
#         },
#         "endpoints": "https://control.cloud-object-storage.cloud.ibm.com/v2/endpoints",
#         "iam_apikey_description": "...",
#         "iam_apikey_name": "Service credentials-1",
#         "iam_role_crn": "crn:v1:bluemix:public:iam::::serviceRole:Writer",
#         "iam_serviceid_crn": "...",
#         "resource_instance_id": "..."
#     }
# }



config = {'lithops': {'backend': 'ibm_cf', 'storage': 'ibm_cos'},

          'ibm_cf': {'endpoint': 'https://us-south.functions.cloud.ibm.com',
                     'namespace': 'roy.rubin@post.idc.ac.il_dev',
                     'api_key': '1defbac0-eea1-4bb8-b5d4-cee7e63b3bb4:63a0ls32DAGjVe0TkdUcBqcOL7lOtR7bLsQYf98WGgW2xpp9Bpd0BUSubnlsfNQM',
                     },
          'ibm_cos': {'storage_bucket': 'cloud-object-storage-mq-cos-standard-8s4',
                      'region': 'eu-de',
                      "access_key": "bb21b4d35ef046d19b4f6fd93f39a3a5",
                      "secret_key": "d9db9b67564ec2fc5467820ef8719533cdc2e64a5ce33695"
                      }
          }


import yaml
from lithops import Storage

if __name__ == "__main__":
    st = Storage(config = config)
    
    st.put_object(bucket='cloud-object-storage-mq-cos-standard-8s4',
                  key='test2.txt',
                  body='Hello rock')
    storage_file_dict = st.list_objects('cloud-object-storage-mq-cos-standard-8s4', prefix='myCSV')
    csv = st.get_object(bucket='cloud-object-storage-mq-cos-standard-8s4',
                        key='myCSV1.csv')
    
from lithops.storage.cloud_proxy import os

if __name__ == "__main__":
    filepath = 'bar/foo.txt'
    with os.open(filepath, 'w') as f:
        f.write('Hello world!')

    dirname = os.path.dirname(filepath)
    print(os.listdir(dirname))
    os.remove(filepath)
    
    
    
# if __name__ == '__main__':
#     # fexec = lithops.FunctionExecutor()
#     fexec = lithops.FunctionExecutor(config = config)
#     fexec.call_async(hello_world, 'World')
#     print(fexec.get_result())



