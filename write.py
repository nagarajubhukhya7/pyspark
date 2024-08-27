def to_files(df,tgt_dir,file_format):
    df.coalesce(16). \
        write. \
        partitionBy('year','month','day').  \
        format(file_format). \
        save(tgt_dir)
