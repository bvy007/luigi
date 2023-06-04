import re


def clean_record(text):
    # Remove newline characters and replace with a space
    cleaned_text = text.replace('\n', ' ')

    # Remove tab characters and replace with a space
    cleaned_text = cleaned_text.replace('\t', ' ')

    # Remove other special characters using regular expression
    cleaned_text = re.sub(r'[^\w\s]', '', cleaned_text)

    return cleaned_text


def process_data(data):
    # Implement your data processing logic here
    
    #processed_data = data
    cleaned_data_json = []

    for record in data:

        if not isinstance(record["userId"], int) or not isinstance(record["id"], int):
            record["userId"] = int(record["userId"])
            record["id"] = int(record["id"])

        if not isinstance(record["userId"], str) or not isinstance(record["id"], str):
            record["title"] = str(record["title"])
            record["body"] = str(record["body"])
        
        record["title"] = clean_record(record["title"])
        record["body"] = clean_record(record["body"])

        cleaned_data_json.append(record)

    return cleaned_data_json




