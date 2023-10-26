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

# %%
def get_children_ids(block):
    for rels in block.get("relationships", []):
        if rels["type"] == "CHILD":
            yield from rels["ids"]

# %%
def get_value_ids(block):
    for rels in block.get("relationships", []):
        if rels["type"] == "VALUE":
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

def get_key_value_content(selected_id,keyValueSets,word_block):
    text = ""
    for idx, keyValue in enumerate(keyValueSets.values()):
        #Iterate only on VALUE entity type
        if "VALUE" in keyValue["entityTypes"]:
            child_ids = [child for child in get_children_ids(keyValue)]
           
            #Determine if selected Id is child of KEY_VALUE VALUE block
            if (selected_id in child_ids):
                #Determine KEY entity type for KEY_VALUE_SET
                     
                for idx1, keyValue1 in enumerate(keyValueSets.values()):
                    
                    #Iterate only on VALUE entity type
                    if "KEY" in keyValue1["entityTypes"]:
                        value_ids_list = [child for child in get_value_ids(keyValue1)]
                        
                        #Determine if VALUE in value array list
                        if(keyValue['id'] in value_ids_list):
                            
                            #return the text for child word blocks
                            for relationship in keyValue1['relationships']:
                                if relationship['type'] == 'CHILD':
                                    for child_id in relationship['ids']:
                                        if(child_id in word_block):
                                            word = word_block[child_id]
                                            if(word):
                                                text += word['text'] + " "
    return text.strip()
# %%
def map_blocks(blocks, block_type):
    return {block["id"]: block for block in blocks if block["blockType"] == block_type}

def get_selected_elements(blocks):
    selected_elements = list(filter(lambda sub: sub["blockType"] == "SELECTION_ELEMENT" and sub["selectionStatus"] == "SELECTED", blocks))
    return {element["id"]: element for element in selected_elements}

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

            #response = read_textract_json(jsonFilepath)

            blocks = response["blocks"]
            tables = map_blocks(blocks, "TABLE")
            cells = map_blocks(blocks, "CELL")
            words = map_blocks(blocks, "WORD")
            keyValueSets = map_blocks(blocks, "KEY_VALUE_SET")
            selections = map_blocks(blocks, "SELECTION_ELEMENT")

            #Get all SELECTED elements IDs
            selectedBlocks = get_selected_elements(blocks)
            selectedBlocksIds = list(selectedBlocks.keys())

            # %%
            #Get TABLE which CELL has SELECTED IDs
            print(filename)
            for idx, table in enumerate(tables.values()):
                # Determine all the cells that belong to this table
                table_cells = [cells[cell_id] for cell_id in get_children_ids(table)]

                #print("Correct table", get_cell_content(table_cells[0],words),  table_cells[0]['page'])

                # Determine correct table
                if("COLUMN_HEADER" in table_cells[0]['entityTypes'] and get_cell_content(table_cells[0],words)=="Type"):
                    

                    for cell in table_cells:
                        if(cell['columnIndex']==1):
                            #print(cell)
                            #Get cell childs
                            childs = [cell_child for cell_child in get_children_ids(cell)]
                            childs_set = set(childs)
                            selectedBlocksIds_set = set(selectedBlocksIds)

                            if(childs_set & selectedBlocksIds_set):
                                #print(childs_set & selectedBlocksIds_set)
                                #print(get_cell_content(cell,words))
                                cell_selected_ids = childs_set & selectedBlocksIds_set
                                #Iterate over SET
                                for selected_Ids in cell_selected_ids:
                                    #print(selected_Ids)

                                    #Selected value logic
                                    selectedValueContent = get_key_value_content(selected_Ids,keyValueSets,words)

                                    #exhibit logic
                                    page_list = [cell['page']-2,cell['page']-1,cell['page']]
                                    exhibit = get_exhibit(page_list, response)

                                    #exhibit line logic
                                    exhibit_line = get_exhibit_line(page_list, response)

                                    #reimbursement 1 logic
                                    reimbursementCell = list(filter(lambda sub : sub["columnIndex"] == 3 and sub["rowIndex"] == cell["rowIndex"], table_cells))
                                    reimbursement = get_cell_content(reimbursementCell[0],words)

                                    #reimbursement 2 logic
                                    reimbursementCell2 = list(filter(lambda sub : sub["columnIndex"] == 4 and sub["rowIndex"] == cell["rowIndex"], table_cells))
                                    reimbursement2 = ""
                                    if(len(reimbursementCell2)):
                                        reimbursement2 = get_cell_content(reimbursementCell[0],words)

                                    #Logic to determine services selected
                                    servicesCell = list(filter(lambda sub : sub["columnIndex"] == 2 and sub["rowIndex"] == cell["rowIndex"], table_cells))[0]
                                    servicesCellChilds = [cell_child for cell_child in get_children_ids(servicesCell)]
                                    serviceschilds_set = set(servicesCellChilds)

                                    #Determine if selected services exist
                                    if(serviceschilds_set & selectedBlocksIds_set):
                                        services_selected_ids = serviceschilds_set & selectedBlocksIds_set
                                         #Iterate over SET
                                        for services_selected_Id in services_selected_ids:
                                            #Selected value logic
                                            servicesSelectedValueContent = get_key_value_content(services_selected_Id,keyValueSets,words)
                                            print(filename,exhibit,exhibit_line,selectedValueContent,servicesSelectedValueContent,reimbursement,reimbursement2,cell['page'])
                                            output_data.append([filename, exhibit, exhibit_line, selectedValueContent,servicesSelectedValueContent,reimbursement,reimbursement2,cell['page']])
                                    else:
                                        print(filename,exhibit,exhibit_line,selectedValueContent,"",reimbursement,cell['page'])
                                        output_data.append([filename, exhibit, exhibit_line, selectedValueContent,"",reimbursement,reimbursement2,cell['page']])
    return output_data

#Get associated cells in a row

# Function to write data to a CSV file
def write_to_csv(output_data, output_file):
    with open(output_file, 'w', newline='') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(["Filename", "Exhibit", "Exhibit Heading", "Type","Services","Reimbursement","DNSP Reimbursement","Page"])
        writer.writerows(output_data)

# %%
if __name__ == "__main__":
    #json_folder = 'D:\projects\OCR\client documents\CHC\exhibit-json\exhibit'
    #output_file = 'output.csv'

    json_folder = input("Enter JSON folder path: ")
    output_file = input("Enter output file path: ")
    data = process_textract_files(json_folder)
    write_to_csv(data, output_file)
