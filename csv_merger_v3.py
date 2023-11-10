
#dyanamic csv merge code. 
"""
csv merger version 1.  this code merges the csv files from a folder which contain
multiple folders. its a dynamic code merge and chunk...
"""
#author: aditya 
import os
import pandas as pd

def get_merged_df(folder_path,file_name):
    """>>>>get_merged_df:- takes folder_path and file_name as argument. 
    
    folder_path : contains multiple folders which contain files.
    file_name : file which we want to merge from all folders.
    
    >>>  this function returns a merged dataframe . and 
         write a csv file of row count of each file.
    """
    folder_name = os.listdir(folder_path)
    df_list = [] #df = dataframe
    #--------------------------------
    for folder in folder_name:
        for file in os.listdir(os.path.join(folder_path,folder)):
            if file_name in file: #file name
                file_path = os.path.join(folder_path,folder,file)
                if os.path.exists(file_path):
                    df = pd.read_csv(file_path)
                    df_list.append(df)
            #-------------------------------------------------
                    with open(f'{file_name}_row_count.csv','a') as f:
                        f.write(f'{folder},{df.shape[0]},{df.shape[1]}\n')
                
    return pd.concat(df_list,ignore_index=True)

def get_chunk_files(chunk_size,dataframe,output_folder,file_name):
    """
    >>>> get_chunk_files --- this function takes chunk size , dataframe, output path, file name
    as an arugment and return chunked csv files.
    
    """
    ## so 99.9% , length of df cannot of round number. like 10 . 
    chunks = dataframe.shape[0]//chunk_size
    for i in range(chunks):
        dataframe[chunk_size*i:chunk_size*(i+1)].to_csv(f'{output_folder}\\merged_{file_name}_{i}.csv',index=False) 
                    #0   :   10 #actual 9  i = 0
                    #10 :  20   # actual 19 i = 1
    dataframe[chunk_size*(i+1):].to_csv(f'{output_folder}\\merged_{file_name}_{i+1}.csv',index=False)
                #10 * 2 : end ...  # 10*2 ----> 

        
    print('successfully chunked your files!!!')
        
        
if __name__ == "__main__":
    print('note: add extra backslash in the path.\n\n')
    #..#..#--------------------------------------------------------------
    folder_path = input('copy and paste input folder path: ').strip()
    file_name = input('enter file name which you want to merge: ').strip()
    chunk_size = int(input('enter chunk size of csv files: '))
    output_folder = input('enter output folder path: ').strip()
    #----------------------------------------------------------------------
    merged_dataframe = get_merged_df(folder_path,file_name)
    
    if len(merged_dataframe) < chunk_size:
        merged_dataframe.to_csv(f'{output_folder}\\merged_{file_name}.csv',index=False)
    else:
        get_chunk_files(chunk_size,merged_dataframe,output_folder,file_name)
    #---------------------------------------------------------------------
    print('all process done')
    
