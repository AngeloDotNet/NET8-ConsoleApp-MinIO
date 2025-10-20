using Amazon.S3;
using Amazon.S3.Model;

var hostname = "http://localhost:9000"; // MinIO URL
var username = "username";              // MinIO username
var password = "password";              // MinIO password

Console.WriteLine("Hello, World!");

var s3Client = CreateS3Client();

await CreateBucketAsync(s3Client, "bucket-prova");
await CreateBucketAsync(s3Client, "bucket-prova-1");
await CreateBucketAsync(s3Client, "bucket-prova-2");
await CreateBucketAsync(s3Client, "bucket-prova-3");

await ListBucket(s3Client);

await DeleteBucketAsync(s3Client, "bucket-prova-1");
await DeleteBucketAsync(s3Client, "bucket-prova-2");
await DeleteBucketAsync(s3Client, "bucket-prova-3");

await ListAllObjectsAsync(s3Client, "bucket-prova");

await UploadFileAsync(s3Client, "bucket-prova");

await ListAllObjectsAsync(s3Client, "bucket-prova");

await DownloadObjectAsync(s3Client, "bucket-prova");

await UpdateObjectAsync(s3Client, "bucket-prova"); //Aggiorna il contenuto del file esistente

await ListAllObjectsAsync(s3Client, "bucket-prova");

await DeleteObjectAsync(s3Client, "bucket-prova", "file_to_delete.txt");

await ListAllObjectsAsync(s3Client, "bucket-prova");

// For other operations, you can visit: https://docs.aws.amazon.com/AmazonS3/latest/API/s3_example_s3_RestoreObject_section.html

AmazonS3Client CreateS3Client()
{
    var s3Config = new AmazonS3Config
    {
        ServiceURL = hostname,
        ForcePathStyle = true // Required for MinIO compatibility
    };

    return new AmazonS3Client(username, password, s3Config);
}

async Task ListAllObjectsAsync(AmazonS3Client s3Client, string bucketName)
{
    var listRequest = new ListObjectsV2Request
    {
        BucketName = bucketName
    };

    var listResponse = await s3Client.ListObjectsV2Async(listRequest);

    await Console.Out.WriteLineAsync("All objects in S3");
    await Console.Out.WriteLineAsync("-----------------------------------------------------");

    if (listResponse.S3Objects == null || listResponse.S3Objects.Count == 0)
    {
        Console.Out.WriteLine("No objects found in the bucket.");
    }
    else
    {
        foreach (var obj in listResponse.S3Objects)
        {
            await Console.Out.WriteLineAsync($"Object: {obj.Key}, Size: {obj.Size}");
        }
    }

    await Console.Out.WriteLineAsync("-----------------------------------------------------");
}

async Task UploadFileAsync(AmazonS3Client s3Client, string bucketName)
{
    var dataOra = DateTime.Now.ToString("yyyyMMdd_HHmmss");
    var nomeFile = $"{dataOra}_test_file.txt";
    var dirFiles = Directory.GetCurrentDirectory();
    var filePath = Path.Combine(dirFiles.Replace("\\bin\\Debug\\net8.0", ""), "prova.txt");

    var putRequest = new PutObjectRequest
    {
        BucketName = bucketName,
        Key = nomeFile,
        FilePath = filePath, // Path to the file to upload
        ContentType = "text/plain", // Set the content type
        AutoCloseStream = true, // Automatically close the stream after upload
        TagSet =
        [
            new Tag { Key = "CreatedBy", Value = "ConsoleAppMinIO" },
            new Tag { Key = "CreatedOn", Value = dataOra }
        ]
    };

    await s3Client.PutObjectAsync(putRequest);

    await Console.Out.WriteLineAsync("File has been uploaded");
    await Console.Out.WriteLineAsync("-----------------------------------------------------");
}

async Task DownloadObjectAsync(AmazonS3Client s3Client, string bucketName)
{
    var getRequest = new GetObjectRequest
    {
        BucketName = bucketName,
        Key = "20250809_184250_test_file.txt"
    };

    using var response = await s3Client.GetObjectAsync(getRequest);

    try
    {
        await response.WriteResponseStreamToFileAsync($"{Directory.GetCurrentDirectory()}\\{getRequest.Key}", true, CancellationToken.None);
    }
    catch (AmazonS3Exception ex)
    {
        Console.WriteLine($"Error saving {getRequest.Key}: {ex.Message}");
    }

    // Uncomment the following lines to read the content of the downloaded object but not download it to a file
    //using (var response = await s3Client.GetObjectAsync(getRequest))
    //using (var reader = new StreamReader(response.ResponseStream))
    //{
    //	string content = await reader.ReadToEndAsync();
    //	await Console.Out.WriteLineAsync("Object has been downloaded");
    //	await Console.Out.WriteLineAsync($"Object Content: {content}");
    //	await Console.Out.WriteLineAsync("-----------------------------------------------------");
    //}
}

// This method updates the content of an existing object in the bucket.
static async Task UpdateObjectAsync(AmazonS3Client s3Client, string bucketName)
{
    var updateRequest = new PutObjectRequest
    {
        BucketName = bucketName,
        Key = "20250806_231727_test_file.txt",
        ContentBody = $"Updated Content at {DateTime.Now}"
    };

    await s3Client.PutObjectAsync(updateRequest);
    await Console.Out.WriteLineAsync("Object has been updated");
    await Console.Out.WriteLineAsync("-----------------------------------------------------");
}

async Task DeleteObjectAsync(AmazonS3Client s3Client, string bucketName, string deleteFile)
{
    var deleteRequest = new DeleteObjectRequest
    {
        BucketName = bucketName,
        Key = deleteFile
    };

    await s3Client.DeleteObjectAsync(deleteRequest);
    await Console.Out.WriteLineAsync("Object has been deleted");
    await Console.Out.WriteLineAsync("-----------------------------------------------------");
}

async Task CreateBucketAsync(AmazonS3Client s3Client, string bucketName)
{
    var createBucketRequest = new PutBucketRequest
    {
        BucketName = bucketName,
        UseClientRegion = true // Use the region of the client
    };

    await s3Client.PutBucketAsync(createBucketRequest);
    await Console.Out.WriteLineAsync("Bucket has been created");
    await Console.Out.WriteLineAsync("-----------------------------------------------------");
}

async Task DeleteBucketAsync(AmazonS3Client s3Client, string bucketName)
{
    var deleteBucketRequest = new DeleteBucketRequest
    {
        BucketName = bucketName
    };

    await s3Client.DeleteBucketAsync(deleteBucketRequest);
    await Console.Out.WriteLineAsync("Bucket has been deleted");
    await Console.Out.WriteLineAsync("-----------------------------------------------------");
}

async Task ListBucket(AmazonS3Client s3Client)
{
    var listBucketsResponse = await s3Client.ListBucketsAsync();

    await Console.Out.WriteLineAsync("All buckets in S3");
    await Console.Out.WriteLineAsync("-----------------------------------------------------");

    if (listBucketsResponse.Buckets == null || listBucketsResponse.Buckets.Count == 0)
    {
        Console.Out.WriteLine("No buckets found.");
    }
    else
    {
        foreach (var bucket in listBucketsResponse.Buckets)
        {
            await Console.Out.WriteLineAsync($"Bucket: {bucket.BucketName}, Created on: {bucket.CreationDate}");
        }
    }

    await Console.Out.WriteLineAsync("-----------------------------------------------------");
}
