import os
from langchain.llms.bedrock import Bedrock
from typing import Optional
import boto3
from botocore.config import Config

def get_bedrock_client(
    assumed_role: Optional[str] = None,
    region: Optional[str] = None,
    runtime: Optional[bool] = True,
):
    """Create a boto3 client for Amazon Bedrock, with optional configuration overrides

    Parameters
    ----------
    assumed_role :
        Optional ARN of an AWS IAM role to assume for calling the Bedrock service. If not
        specified, the current active credentials will be used.
    region :
        Optional name of the AWS Region in which the service should be called (e.g. "us-east-1").
        If not specified, AWS_REGION or AWS_DEFAULT_REGION environment variable will be used.
    runtime :
        Optional choice of getting different client to perform operations with the Amazon Bedrock service.
    """
    if region is None:
        target_region = os.environ.get("AWS_REGION", os.environ.get("AWS_DEFAULT_REGION"))
    else:
        target_region = region

    print(f"Create new client\n  Using region: {target_region}")
    session_kwargs = {"region_name": target_region}
    client_kwargs = {**session_kwargs}

    profile_name = os.environ.get("AWS_PROFILE")
    if profile_name:
        print(f"  Using profile: {profile_name}")
        session_kwargs["profile_name"] = profile_name

    retry_config = Config(
        region_name=target_region,
        retries={
            "max_attempts": 10,
            "mode": "standard",
        },
    )
    session = boto3.Session(**session_kwargs)

    if assumed_role:
        print(f"  Using role: {assumed_role}", end='')
        sts = session.client("sts")
        response = sts.assume_role(
            RoleArn=str(assumed_role),
            RoleSessionName="langchain-llm-1"
        )
        print(" ... successful!")
        client_kwargs["aws_access_key_id"] = response["Credentials"]["AccessKeyId"]
        client_kwargs["aws_secret_access_key"] = response["Credentials"]["SecretAccessKey"]
        client_kwargs["aws_session_token"] = response["Credentials"]["SessionToken"]

    if runtime:
        service_name='bedrock-runtime'
    else:
        service_name='bedrock'

    bedrock_client = session.client(
        service_name=service_name,
        config=retry_config,
        **client_kwargs
    )

    print("boto3 Bedrock client successfully created!")
    print(bedrock_client._endpoint)
    return bedrock_client


def process_file_content(file,query_name ):

    os.environ["AWS_DEFAULT_REGION"] = "us-west-2"  # E.g. "us-east-1"
    os.environ["AWS_PROFILE"] = "default"


    boto3_bedrock = get_bedrock_client(
        region=os.environ.get("AWS_DEFAULT_REGION", None),
    )

    llm = Bedrock(
        model_id="anthropic.claude-v2",
        client=boto3_bedrock,
        model_kwargs={
            "max_tokens_to_sample": 400,
            "temperature": 0, # Using 0 to get reproducible results
            "stop_sequences": ["\n\nHuman:"]
        }
    )
    raw_text = file
    
    

    # query = f"""

    # Human: Given is json file data of  AWS cost explorer please read it and analyse the contents.
    # according to the data, give me insights which can help me reduce costs in my AWS account and return key findings like where my cost is increasing and what made my cost decrease.
    # Give me services that can be optimized .
    

    # cost explorer aws account data: ```
    # {raw_text}
    # ```

    # Assistant:"""

    query = f"""

    Human: given is database related information for {query_name}.please analyze the data.
    According to the data, give me insights which can help me improve the database performance and optimize queries.
    
    

    database data: ```
    {raw_text}
    ```

    Assistant:"""
    # print(f"{query =}")

    result = llm(query)
    print(f"{result = }")
    insights_data = (result.strip())


    
    
    return insights_data


def generate_insights(data, query_name = None,folder= None):
    insights_data = process_file_content(data, query_name)

    folder = os.path.join('..', folder)
    os.makedirs(folder, exist_ok=True)  
    insights_file_path = os.path.join(folder, query_name+".txt")
    print(f"writing inisght to {insights_file_path}")
    with open(f"{insights_file_path}",'w') as file_data:
        file_data.write(insights_data)