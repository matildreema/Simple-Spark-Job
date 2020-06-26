import com.amazonaws.auth.{InstanceProfileCredentialsProvider, STSAssumeRoleSessionCredentialsProvider}
import com.amazonaws.regions.Regions
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest
import com.amazonaws.services.securitytoken.{AWSSecurityTokenService, AWSSecurityTokenServiceClientBuilder}
import com.amazonaws.util.EC2MetadataUtils
import com.twitter.scalding.Args
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

class SparkJob{
  def solve(inputDF:DataFrame) = {
    val outputDF = inputDF.where(col("Id") === "2")
    outputDF
  }
}


object SparkJob {

  def configureSecretKey(configuration: Configuration,
                         stsClient: AWSSecurityTokenService,
                         roleARN: String,
                         roleSessionName: String
                        ) = {
    val provider = new STSAssumeRoleSessionCredentialsProvider.Builder(
      roleARN, roleSessionName).withStsClient(stsClient).build()

    provider.getCredentials.getSessionToken
    provider.getCredentials.getAWSSecretKey
    provider.getCredentials.getAWSAccessKeyId
    val request = new AssumeRoleRequest()
    println(s"CURRENT ARN :: ${request.getRoleArn}")

    request.setRoleArn(roleARN)
    request.setRoleSessionName(roleSessionName)

    println(s"ASSUMING ARN :: ${request.getRoleArn}")

    val assumeRoleResult = stsClient.assumeRole(request)

    println(s"ASSUMED USER :: ${assumeRoleResult.getAssumedRoleUser}")

    configuration.set("fs.s3a.access.key", assumeRoleResult.getCredentials().getAccessKeyId())
    configuration.set("fs.s3a.secret.key", assumeRoleResult.getCredentials().getSecretAccessKey())
    configuration.set("fs.s3a.session.token", assumeRoleResult.getCredentials().getSessionToken())
  }

  def main(args: Array[String]): Unit = {

    val logger = LoggerFactory.getLogger(this.getClass)

    val cmdArgs = Args(args)
    val input_path = cmdArgs("input_file")
    val output_path = cmdArgs("output_file")
    val roleArnRead = cmdArgs("roleArnRead")
    val roleArnWrite =  cmdArgs("roleArnWrite")

    val clientRegion = Regions.US_EAST_1

    logger.info(s"Input file: ${input_path}, Output file: ${output_path}")

    val sparkSession = SparkSession.builder()
      .appName("Spark Job")
      .getOrCreate()

    logger.info("Loading STS Temporary credentials....")
    val roleSessionName: String = EC2MetadataUtils.getInstanceId() + "Session"

    val credentialsProvider = new InstanceProfileCredentialsProvider(true)

    val stsClient: AWSSecurityTokenService = AWSSecurityTokenServiceClientBuilder
      .standard()
      .withCredentials(credentialsProvider)
      .withRegion(clientRegion)
      .build()

    val configuration = new Configuration()
//    configuration.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
    configureSecretKey(configuration, stsClient, roleArnRead, roleSessionName)

    logger.info("Fetching input data from S3 bucket");
    val schema = StructType(Array(
      StructField("Id", StringType),
      StructField("Item", StringType)
    ))

    val inputDF = sparkSession.read
      .schema(schema)
      .option("header", "true")
      .option("sep", ",")
      .csv(input_path)
    val outputDF = new SparkJob().solve(inputDF)

//    configureSecretKey(configuration, stsClient, roleArnWrite, roleSessionName)
    logger.trace("Storing output data back to S3 bucket")
    outputDF.write
        .format("csv")
        .mode(SaveMode.Overwrite)
        .save(output_path)

    logger.info(String.format("Exiting application, byee...."))

  }

}
