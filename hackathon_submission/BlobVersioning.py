from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceExistsError
import os

connect_str = '<PUT_YOUR_CONNECTION_STRING>'

blob_service_client = BlobServiceClient.from_connection_string(connect_str)
container_name = 'cont1'
try:
    blob_service_client.create_container(container_name)
except ResourceExistsError:
    print('Container already exists.')


local_path = "./data"

## Create 3 files with different contents that will be used to upload the data
local_file_name =  "file1.txt"
upload_file_path = os.path.join(local_path, local_file_name)

# Write text to the file
file = open(upload_file_path, 'w')
file.write("Hello, World!")
file.close()


local_file_name =  "file2.txt"
upload_file_path = os.path.join(local_path, local_file_name)

# Write text to the file
file = open(upload_file_path, 'w')
file.write("Hello, World!-- Version 2")
file.close()


local_file_name =  "file3.txt"
upload_file_path = os.path.join(local_path, local_file_name)

# Write text to the file
file = open(upload_file_path, 'w')
file.write("Hello, World!-- Version 3")
file.close()

# Upload the files to the same blob to make 3 versions of the blob
file_name = 'file1.txt'
blob_name = 'test_blob1'
blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
file_path = os.path.join(local_path, file_name)
with open(file_path, "rb") as data:
    blob_client.upload_blob(data, overwrite='True')

file_name = 'file2.txt'
blob_name = 'test_blob1'
blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
file_path = os.path.join(local_path, file_name)
with open(file_path, "rb") as data:
    blob_client.upload_blob(data, overwrite='True')

file_name = 'file3.txt'
blob_name = 'test_blob1'
blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
file_path = os.path.join(local_path, file_name)
with open(file_path, "rb") as data:
    blob_client.upload_blob(data, overwrite='True')


def list_blob_with_version(blob_name, container_name):
    '''
    List blobs with version
    :param blob_name: Name of the blob
    :param container_name: Name of the container
    :return:
    '''
    cont_client = blob_service_client.get_container_client(container_name)
    page_items = cont_client.list_blobs(name_starts_with=blob_name, include=['versions'])

    for blobs_name in page_items:
        print(blobs_name.name + ' - ' + blobs_name.version_id + ' - ' + str(blobs_name.is_current_version))


list_blob_with_version('test_blob1', container_name)

def delete_blob_with_version(blob_name,container_name, version_id):
    '''
    Delete blob with specific version
    :param blob_name: Name of the blob
    :param container_name: Name of the container
    :param version_id: Version id of the blob to be deleted
    :return:
    '''
    cont_client = blob_service_client.get_container_client(container_name)
    cont_client.delete_blob(blob=blob_name, version_id=version_id)

delete_blob_with_version('test_blob1',container_name,'2021-02-05T22:49:22.1767578Z')

list_blob_with_version('test_blob1', container_name)


