# SnowflakeIO for Beam
This project contains [Beam IO](https://beam.apache.org/documentation/io/built-in/) for [Snowflake](https://www.snowflake.com/). 
It uses [Snowflake JDBC Driver](https://github.com/snowflakedb/snowflake-jdbc).
 
## Authentication
All authentication methods available in Snowflake JDBC Driver are possible to use with he IO:
* username-password combination
* key-pair
* OAuth token

Passing credentials is done via Pipeline Options.

### Username and password
To use username/password authentication in SnowflakeIO invoke your pipeline with following Pipeline Options: 
```
--username=<USERNAME> --password=<PASSWORD
```
and set following properties on your DataSource
```
SnowflakeIO.DataSourceConfiguration.create()
        [...]
        .withUsername(options.getUsername())
        .withPassword(options.getPassword())
        [...]
```

### Key pair

**Note**: 
Before using this method, a key pair has to be generated and Snowflake's user has to be altered with public key.
Please refer to [Snowflake JDBC Driver documentation](https://docs.snowflake.net/manuals/user-guide/jdbc-configure.html#using-key-pair-authentication) 
for more details and instructions how to generate key pair and how to upload pubkey to Snowflake.

To use key-pair authentication in SnowflakeIO invoke your pipeline with following Pipeline Options: 
```
--username=<USERNAME> --privateKeyPath=<PATH_TO_P8_FILE> --privateKeyPassphrase=<PASSWORD_FOR_KEY>
```
and set following properties on your DataSource
```
SnowflakeIO.DataSourceConfiguration.create()
        [...]
        .withUsername(options.getUsername())
        .withPrivateKey(PRIVATE_KEY_OBJECT)
        [...]
```
with `PRIVATE_KEY_OBJECT` being an instance of `java.security.PrivateKey` with your key converted to Java object.


### OAuth token
SnowflakeIO also supports OAuth token.  

**IMPORTANT**: SnowflakeIO requires valid OAuth access token. It will no be able to refresh the token nor obtain it 
using web-based flow. Please refer to [Snowflake documentation](https://docs.snowflake.net/manuals/user-guide/oauth-intro.html)
on how to create an OAuth integration and obtain the token.

Once you have the token, invoke your pipeline with following Pipeline Options: 
```
--oauthToken=<TOKEN>
```
and set following properties on your DataSource
```
SnowflakeIO.DataSourceConfiguration.create()
        [...]
        .withOauthToken(options.getOauthToken()) 
        [...]
```