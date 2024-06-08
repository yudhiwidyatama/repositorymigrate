import sys
import csv
import json
import boto3

def extract_digests(parsed_content):
    try:
        # Extract the config digest
        config_digest = parsed_content["config"]["digest"]

        # Extract the layer digests
        layer_digests = [layer["digest"] for layer in parsed_content["layers"]]

        return config_digest, layer_digests
    except KeyError:
        return None, []

def read_s3_content_from_csv():
    # Replace with your actual credentials and endpoint
    endpoint_url = "http://endpoint:9020"
    access_key = "docker"
    secret_key = "secret"
    bucket_name = "bucket"
    # oc get is -o go-template='{{range .items}}{{$ns := .metadata.namespace}}{{$nm := .metadata.name}}{{range .status.tags}}{{$tag := .tag}}{{ range .items}}{{$ns}},{{$nm}},{{$tag}},{{
.image}},{{.dockerImageReference}}{{"\n"}}{{end}}{{end}}{{end}}' --all-namespaces  > osh-all-repos.csv
    try:
        # Read CSV input from standard input
        reader = csv.reader(sys.stdin)
        s3 = boto3.resource("s3", endpoint_url=endpoint_url, aws_access_key_id=access_key, aws_secret_access_key=secret_key)
        for row in reader:
            namespace, repository, tag, object_key, docker_ref = row

            # Process only rows where docker_ref starts with "172"
            if docker_ref.startswith("172"):
                # Read the content
                try:
                   object_key1 = "docker/registry/v2/blobs/sha256/" + object_key[7:9] + "/" + object_key[7:] + "/data"
                   s3_object = s3.Object(bucket_name, object_key1)
                   content = s3_object.get()["Body"].read().decode("utf-8")

                   # Assuming the content is JSON-formatted, parse it
                   parsed_content = json.loads(content)

                   # Extract digests
                   config_digest, layer_digests = extract_digests(parsed_content)
                   if config_digest:
                      # Print CSV-formatted output
                      print(f"{namespace},{repository},{tag},{config_digest}")
                   for layer_digest in layer_digests:
                      print(f"{namespace},{repository},{tag},{layer_digest}")
                except Exception as e1:
                   print(f"Error accessing S3,{namespace}/{repository}:{tag},{object_key1},{e1}",file=sys.stderr)
    except Exception as e:
        print(f"Error accessing S3: {e}",file=sys.stderr)

if __name__ == "__main__":
    read_s3_content_from_csv()
