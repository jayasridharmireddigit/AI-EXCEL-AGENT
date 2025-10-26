import pandas as pd
import os

input_file = 'C:\\Users\\ADMIN\\Desktop\\AI_XL_Agent\\data\\multi_header_test.xlsx'

directory, filename = os.path.split(input_file)
name, ext = os.path.splitext(filename)
output_file = os.path.join(directory, f"{name}_transformed{ext}")

df = pd.read_excel(input_file, header=[0, 1], index_col=0)

if ('FY24', 'Q1') in df.columns and ('FY24', 'Q2') in df.columns:
    df[('FY24', 'Q3')] = df[('FY24', 'Q1')] + df[('FY24', 'Q2')]

    cols_without_new_q3 = [col for col in df.columns.tolist() if col != ('FY24', 'Q3')]

    insert_index = -1
    try:
        insert_index = cols_without_new_q3.index(('FY24', 'Q2')) + 1
    except ValueError:
        insert_index = len(cols_without_new_q3)

    final_cols_order = cols_without_new_q3[:insert_index] + [('FY24', 'Q3')] + cols_without_new_q3[insert_index:]
    
    df = df[final_cols_order]

df.to_excel(output_file, index=True, header=True)