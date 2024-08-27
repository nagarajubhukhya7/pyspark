import os
from util import get_spark_session
from read import from_files
from process import   transform
from write import to_files
def main():
    env = os.environ.get('ENVIRON')
    src_dir = os.environ.get('SRC_DIR')
    file_pattern = f'{os.environ.get("FILE_PATTERN")}'
    src_file_format = os.environ.get('SRC_FILE_FORMAT')
    tgt_dir = os.environ.get('TGT_DIR')
    tgt_file_format = os.environ.get('TGT_FILE_FORMAT')
    spark = get_spark_session(env,'job')
    df = from_files(spark,src_dir,file_pattern,src_file_format)
    df_transformed = transform(df)
    to_files(df_transformed,tgt_dir,tgt_file_format)


if __name__ =='__main__':
    main()

#spa-submit comand
#you need to zip all the files and to run single file in from zip file use --py-files
#spark-submit --master yarn  --py-files zipfile_name.zip app.py

