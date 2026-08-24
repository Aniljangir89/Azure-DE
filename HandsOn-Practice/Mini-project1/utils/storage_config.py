def configure_storage():
    from pyspark.dbutils import DBUtils
    dbutils = DBUtils(spark)

    storage_key = dbutils.secrets.get(
        scope="azure-kv-scope",
        key="storage-account-key"
    )

    print("Secret retrieved successfully")

    spark.conf.set(
        "fs.azure.account.key.secondstorage89.dfs.core.windows.net",
        storage_key
    )

    print("Key set successfully")