import json

def keep_last_line_only(file_path="dags/topics/logTopics.txt"):
    try:
        # Read all lines from the file
        with open(file_path, 'r', encoding='utf-8') as file:
            lines = file.readlines()

        # Check if there are more than two lines
        if len(lines) > 1:
            # Extract the last line
            last_line = lines[-1].strip()

            # Write only the last line back to the file
            with open(file_path, 'w', encoding='utf-8') as file:
                file.write(last_line + '\n')
                print("Only the last line kept in logTopics.txt:", last_line)
        else:
            print("No action needed. File has one or zero lines.")

    except FileNotFoundError:
        print(f"Error: File '{file_path}' not found.")

def get_next_topic(json_file_path='dags/topics/topic.json'):
    try:
        with open(json_file_path, 'r', encoding='utf-8') as file:
            topics = json.load(file)
            log_value = read_from_log()
            print("Log value:", log_value)
            print("Number of topics:", len(topics.keys()))

            if len(topics.keys()) == log_value:
                #keep_last_line_only()
                write_to_log(new_line="0")
                print("Reset log to 0. No action needed.")
                return {}

            else:
                key_index = log_value
                key = list(topics.keys())[key_index]
                #keep_last_line_only()
                write_to_log(new_line=str(log_value + 1))
                print("Returning topic:", {key: topics[key]})
                return {key: topics[key]}

    except FileNotFoundError:
        print(f"Error: File '{json_file_path}' not found.")
        return {}

def write_to_log(file_path="dags/topics/logTopics.txt", new_line="0"):
    try:
        with open(file_path, 'a', encoding='utf-8') as file:
            file.write(new_line + '\n')  # Append the new line of text followed by a newline character
            print("New line added to logTopics.txt:", new_line)
    except FileNotFoundError:
        print(f"Error: File '{file_path}' not found.")

def read_from_log(file_path="dags/topics/logTopics.txt"):
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            lines = file.readlines()
            if lines:
                last_line = lines[-1].strip()
                return int(last_line)
            else:
                return 0
    except FileNotFoundError:
        print(f"Error: File '{file_path}' not found.")
        return None

def keep_last_line_only(file_path="dags/topics/logTopics.txt"):
    try:
        # Read all lines from the file
        with open(file_path, 'r', encoding='utf-8') as file:
            lines = file.readlines()

        # Check if there are more than two lines
        if len(lines) > 1:
            # Extract the last line
            last_line = lines[-1].strip()

            # Write only the last line back to the file
            with open(file_path, 'w', encoding='utf-8') as file:
                file.write(last_line + '\n')
                print("Only the last line kept in logTopics.txt:", last_line)
        else:
            print("No action needed. File has one or zero lines.")

    except FileNotFoundError:
        print(f"Error: File '{file_path}' not found.")

def save_json_to_temp_file(data):
    """
    Saves JSON data to a temporary file and returns the path of the temporary file.

    Args:
        data (dict): Dictionary containing JSON serializable data.

    Returns:
        str: Path of the temporary file where JSON data is stored.
    """
    try:
        # Create a temporary file
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as temp_file:
            # Write JSON data to the temporary file
            json.dump(data, temp_file, indent=4)
            temp_file.flush()  # Flush to ensure data is written to the file
            temp_file.seek(0)  # Move file pointer to the beginning

            # Get the temporary file path
            temp_file_path = temp_file.name

            return temp_file_path

    except Exception as e:
        print(f"Error occurred while saving JSON to temporary file: {e}")
        return None

tweet_topics = get_next_topic()

print(list(tweet_topics.keys())[0])