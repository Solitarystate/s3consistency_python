# s3consistency.py

The `s3consistency.py` script is designed to check the consistency of files stored in an Amazon S3 bucket. It compares the metadata of files in the bucket with the actual file content to ensure that there are no discrepancies or data corruption.

## Usage

To use this script, follow these steps:

1. Clone or fork this project to your local machine.
2. Ensure that you have the necessary dependencies installed. You can find the required dependencies in the `requirements.txt` file.
3. Configure the script by modifying the `config.py` file. Provide your AWS access key, secret key, and the name of the S3 bucket you want to check.
4. Run the script using the command `python s3consistency.py`.
5. The script will analyze the files in the specified S3 bucket and display the consistency status for each file.
6. The --endpoint defaults to a specific address that you might need to change if you are testing against your local s3 or the AWS S3

## Important Notes

- Make sure you have the necessary permissions to access the specified S3 bucket.
- It is recommended to run this script on a machine with sufficient resources, as it may require significant processing power and memory depending on the size of the bucket.
- This script is provided as-is, without any warranties or guarantees. The author is not responsible for any data loss or damage that may occur as a result of using this script.

## Credits

This project was created by Sudeesh Varier. If you use or reference this project, please provide proper credit to the author.
