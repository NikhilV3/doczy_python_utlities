import boto3
import os
import re
import csv
import json

# %%
# Function to read Textract JSON file
def read_textract_json(json_file):
    #print(json_file)
    with open(json_file, 'r', encoding="utf-8") as file:
        response = json.load(file)
    return response

# Function to write data to a CSV file
def write_to_csv(output_data, output_file):
    with open(output_file, 'w', newline='') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(["Filename", "Exhibit", "Exhibit Heading", "Columns","Services","Reimbursement","DNSP Reimbursement","Page"])
        writer.writerows(output_data)

# %%
def map_blocks(blocks, block_type):
    return {block["id"]: block for block in blocks if block["blockType"] == block_type}


# %%
def get_children_ids(block):
    for rels in block.get("relationships", []):
        if rels["type"] == "CHILD":
            yield from rels["ids"]
# %%
def get_cell_content(cell,word_block):
    cell_text = ""
    for relationship in cell['relationships']:
        if relationship['type'] == 'CHILD':
            for child_id in relationship['ids']:
                if(child_id in word_block):
                    word = word_block[child_id]
                    if(word):
                        cell_text += word['text'] + " "
    return cell_text.strip()

def find_max_value_in_list_of_dicts(data, key_to_maximize):
    # Initialize the maximum value with negative infinity
    max_value = float('-inf')

    # Iterate over the list of dictionaries
    for item in data:
        # Get the value for the specified key
        value = item.get(key_to_maximize, 0)  # 0 is a default value if the key is missing
        # Update the maximum value if the current value is greater
        if value > max_value:
            max_value = value

    return max_value

def get_exhibit(page_list, response):
    line1_regex = r'^Exhibit|EXHIBIT'
    for page in reversed(page_list):
        
        #page_list = [cell['page']]
        line_block = list(filter(lambda sub: sub["page"] == page and sub["blockType"] == "LINE", response['blocks']))

        #iterate only on 1 line items
        for block in line_block[0:10]:
            if block['blockType'] == 'LINE' and 'text' in block:
                text = block['text']
                if re.search(line1_regex, text):
                    return text
                
def get_exhibit_line(page_list, response):
    line1_regex = r'^Exhibit|EXHIBIT'
    for page in reversed(page_list):
        
        #page_list = [cell['page']]
        line_block = list(filter(lambda sub: sub["page"] == page and sub["blockType"] == "LINE", response['blocks']))

        #iterate only on 1 line items
        for i, block in enumerate(line_block[0:10]):
            if block['blockType'] == 'LINE' and 'text' in block:
                text = block['text']
                if re.search(line1_regex, text):
                    return line_block[i+1]['text']
                
# %%
# Function to process Textract JSON files
def process_textract_files(json_folder):
    output_data = []

    for filename in os.listdir(json_folder):
        if filename.endswith('.json'):
            response = read_textract_json(os.path.join(json_folder, filename))

            blocks = response["blocks"]
            tables = map_blocks(blocks, "TABLE")
            cells = map_blocks(blocks, "CELL")
            words = map_blocks(blocks, "WORD")
            keyValueSets = map_blocks(blocks, "KEY_VALUE_SET")
            selections = map_blocks(blocks, "SELECTION_ELEMENT")

            for idx, table in enumerate(tables.values()):
                # Determine all the cells that belong to this table
                table_cells = [cells[cell_id] for cell_id in get_children_ids(table)]

                #print("Correct table", get_cell_content(table_cells[0],words),  table_cells[0]['page'])
                columns = find_max_value_in_list_of_dicts(table_cells,"columnIndex")
                rows = find_max_value_in_list_of_dicts(table_cells,"rowIndex")
                
                # Determine correct table
                if(("COLUMN_HEADER" in table_cells[0]['entityTypes'] and get_cell_content(table_cells[0],words) in ["HCPC","Code","Visit Type","Description","Service","Levels","Service Description","HCPC Code"]) or ("TABLE_TITLE" in table_cells[0]['entityTypes']  and columns == 3)):

                    
                    #exhibit logic to search on 4 previous pages
                    page_list = [table_cells[0]['page']-3,table_cells[0]['page']-2,table_cells[0]['page']-1,table_cells[0]['page']]
                    exhibit = get_exhibit(page_list, response)

                    #exhibit line logic
                    exhibit_line = get_exhibit_line(page_list, response)
                    #print(filename,columns,rows)

                    for i in range(1,rows+1):
                        rowData = [filename,exhibit,exhibit_line,columns]
                        for j in range(1,columns+1):
                            #get cell
                            cell = list(filter(lambda sub: sub["rowIndex"] == i and sub["columnIndex"] == j, table_cells))[0]


                            content = get_cell_content(cell,words)
                            rowData.append(content)
                            #print(j,columns)
                            if(j==columns):
                                #print(cell['page'])
                                rowData.append(cell['page'])
                        print(rowData)
                        output_data.append(rowData)

    return output_data                 

# %%
if __name__ == "__main__":
    #json_folder = 'D:\projects\OCR\client documents\CHC\exhibit-json\exhibit'
    #output_file = 'output.csv'

    json_folder = input("Enter JSON folder path: ")
    output_file = input("Enter output file path: ")
    data = process_textract_files(json_folder)
    write_to_csv(data, output_file)