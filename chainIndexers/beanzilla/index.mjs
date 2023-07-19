import { ECSClient, ListServicesCommand, UpdateServiceCommand } from "@aws-sdk/client-ecs";

export const handler = async (event) => {
  const clusterName = "moonbeans";
  const serviceName = "indexer";

  // Create an ECS service client with the correct region
  const client = new ECSClient({
    region: "us-east-1"
  });

  try {
    // Get the list of services in the cluster
    const listServicesInput = {
      cluster: clusterName,
      launchType: "EC2",
    };
    const listServicesCommand = new ListServicesCommand(listServicesInput);
    const listServicesResponse = await client.send(listServicesCommand);

    // Find the ARN of the service by name
    const serviceArns = listServicesResponse.serviceArns;
    const serviceArn = serviceArns.find((arn) => arn.includes(serviceName));
    
    console.log(serviceArn);

    if (serviceArn) {
      // // Restart the service
      const updateServiceInput = {
        cluster: clusterName,
        service: serviceArn,
        forceNewDeployment: true,
      };
      const updateServiceCommand = new UpdateServiceCommand(updateServiceInput);
      const updateResult = await client.send(updateServiceCommand);

      console.log(updateResult);
      
      return {
        statusCode: 200,
        body: 'Service restart initiated successfully.',
      };
    } else {
      return {
        statusCode: 404,
        body: `Service '${serviceName}' not found in cluster '${clusterName}'.`,
      };
    }
  } catch (error) {
    console.error('Error:', error);

    return {
      statusCode: 500,
      body: 'An error occurred while restarting the service.',
    };
  }
};