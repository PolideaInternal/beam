# SnowflakeIO for Beam
This project contains [Beam IO](https://beam.apache.org/documentation/io/built-in/) transforms for reading data from 
and writing data to [Snowflake](https://www.snowflake.com/) tables. The SnowflakeIO transforms use the 
[Snowflake JDBC Driver](https://github.com/snowflakedb/snowflake-jdbc).

For more information about Snowflake, see the [Snowflake documentation](https://docs.snowflake.net/manuals/index.html).
 
## Authentication
All authentication methods available for the Snowflake JDBC Driver are possible to use with the IO transforms:
* Username and password
* Key pair
* OAuth token

Passing credentials is done via Pipeline options.

### Username and password
To use username/password authentication in SnowflakeIO, invoke your pipeline with the following Pipeline options:
```
--username=<USERNAME> --password=<PASSWORD
```
and set the following properties on your DataSource:
```
SnowflakeIO.DataSourceConfiguration.create()
        [...]
        .withUsername(options.getUsername())
        .withPassword(options.getPassword())
        [...]
```

### Key pair

**Note**: 
To use this authentication method, you must first generate a key pair and associate the public key with the Snowflake 
user that will connect using the IO transform. For instructions,  see the [Snowflake documentation](https://docs.snowflake.net/manuals/user-guide/jdbc-configure.html#using-key-pair-authentication).

To use key pair authentication with SnowflakeIO, invoke your pipeline with following Pipeline options:
```
--username=<USERNAME> --privateKeyPath=<PATH_TO_P8_FILE> --privateKeyPassphrase=<PASSWORD_FOR_KEY>
```
and set following properties on your DataSource:
```
SnowflakeIO.DataSourceConfiguration.create()
        [...]
        .withUsername(options.getUsername())
        .withPrivateKey(PRIVATE_KEY_OBJECT)
        [...]
```
Where `PRIVATE_KEY_OBJECT` is the private key as a Java object, an instance of the `java.security.PrivateKey` class.


### OAuth token
SnowflakeIO also supports OAuth token.  

**IMPORTANT**: SnowflakeIO requires a valid OAuth access token. It will neither be able to refresh the token nor obtain 
it using a web-based flow. For information on configuring an OAuth integration and obtaining the token, see the 
[Snowflake documentation](https://docs.snowflake.net/manuals/user-guide/oauth-intro.html).

Once you have the token, invoke your pipeline with following Pipeline Options: 
```
--oauthToken=<TOKEN>
```
and set following properties on your DataSource:
```
SnowflakeIO.DataSourceConfiguration.create()
        [...]
        .withOauthToken(options.getOauthToken()) 
        [...]
```