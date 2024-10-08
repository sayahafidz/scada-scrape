import re
import threading
import time
import websocket
from datetime import datetime
import pyodbc


# Create a mapping of id to name
id_to_name = {
    46: "PLC_BPV_MAKE_UP_STEAM.Make_Up_AM",
    47: "PLC_BPV_MAKE_UP_STEAM.Blow_Off_AM",
    48: "PLC_BPV_MAKE_UP_STEAM.Tekanan_Boiler",
    49: "PLC_BPV_MAKE_UP_STEAM.Tekanan_BPV",
    54: "PLC_BPV_MAKE_UP_STEAM.Output_MUP",
    55: "PLC_BPV_MAKE_UP_STEAM.Output_BOFF",
    58: "PLC_STERILIZER.S1_Inlet",
    59: "PLC_STERILIZER.S1_Exhaust",
    60: "PLC_STERILIZER.S1_Condenssate",
    61: "PLC_STERILIZER.S1_Mode",
    62: "PLC_STERILIZER.S1_Door",
    71: "PLC_STERILIZER.S1_Press",
    72: "PLC_STERILIZER.S1_Temp",
    73: "PLC_STERILIZER.S2_Inlet",
    74: "PLC_STERILIZER.S2_Exhaust",
    75: "PLC_STERILIZER.S2_Cond",
    76: "PLC_STERILIZER.S2_Mode",
    77: "PLC_STERILIZER.S2_Door",
    86: "PLC_STERILIZER.S2_Press",
    87: "PLC_STERILIZER.S2_Temp",
    88: "PLC_STERILIZER.S3_Inlet",
    89: "PLC_STERILIZER.S3_Exhaust",
    90: "PLC_STERILIZER.S3_Cond",
    91: "PLC_STERILIZER.S3_Mode",
    92: "PLC_STERILIZER.S3_Door",
    101: "PLC_STERILIZER.S3_Press",
    102: "PLC_STERILIZER.S3_Temp",
    116: "PM_Turbin2.Total_Active_Power1",
    137: "PM_Turbin3.Total_Active_Power1",
    157: "PM_Genset1.Total_Active_Power1",
    176: "PLC_PRESS_THRESHER.Press1_Run",
    177: "PLC_PRESS_THRESHER.Digester1_Run",
    178: "PLC_PRESS_THRESHER.Press2_Run",
    179: "PLC_PRESS_THRESHER.Digester2_Run",
    180: "PLC_PRESS_THRESHER.Press3_Run",
    181: "PLC_PRESS_THRESHER.Digester3_Run",
    182: "PLC_PRESS_THRESHER.Press4_Run",
    183: "PLC_PRESS_THRESHER.Digester4_Run",
    184: "PLC_PRESS_THRESHER.Thresher1_Run",
    185: "PLC_PRESS_THRESHER.Thresher2_Run",
    186: "PLC_PRESS_THRESHER.Thresher3_Run",
    187: "PLC_PRESS_THRESHER.Ampere_Press1",
    188: "PLC_PRESS_THRESHER.Ampere_Digester1",
    189: "PLC_PRESS_THRESHER.Ampere_Press2",
    190: "PLC_PRESS_THRESHER.Ampere_Digester2",
    191: "PLC_PRESS_THRESHER.Ampere_Press3",
    192: "PLC_PRESS_THRESHER.Ampere_Digester3",
    193: "PLC_PRESS_THRESHER.Ampere_Press4",
    194: "PLC_PRESS_THRESHER.Ampere_Digester4",
    195: "PLC_PRESS_THRESHER.Temp_Digester1",
    196: "PLC_PRESS_THRESHER.Temp_Digester2",
    197: "PLC_PRESS_THRESHER.Temp_Digester3",
    198: "PLC_PRESS_THRESHER.Temp_Digester4",
    203: "PLC_PRESS_THRESHER.Ampere_Thresher1",
    204: "PLC_PRESS_THRESHER.Ampere_Thresher2",
    205: "PLC_PRESS_THRESHER.Ampere_Thresher3",
    210: "PLC_KLARIFIKASI.Oil_Separator1",
    211: "PLC_KLARIFIKASI.Oil_Separator2",
    212: "PLC_KLARIFIKASI.Oil_Separator3",
    213: "PLC_KLARIFIKASI.Oil_Separator4",
    214: "PLC_KLARIFIKASI.Motoran_1",
    215: "PLC_KLARIFIKASI.Motoran_2",
    216: "PLC_KLARIFIKASI.Motoran_3",
    217: "PLC_KLARIFIKASI.Motoran_4",
    219: "PLC_KLARIFIKASI.Motoran_6",
    220: "PLC_KLARIFIKASI.Motoran_7",
    221: "PLC_KLARIFIKASI.Motoran_8",
    222: "PLC_KLARIFIKASI.Motoran_9",
    223: "PLC_KLARIFIKASI.Ampere_OS1",
    224: "PLC_KLARIFIKASI.Ampere_OS2",
    225: "PLC_KLARIFIKASI.Ampere_OS3",
    226: "PLC_KLARIFIKASI.Ampere_OS4",
    227: "PLC_KLARIFIKASI.HRM_OS1",
    228: "PLC_KLARIFIKASI.HRM_OS2",
    229: "PLC_KLARIFIKASI.HRM_OS3",
    230: "PLC_KLARIFIKASI.HRM_OS4",
    248: "PLC_BOILER_1.FinalPV_WLTX",
    249: "PLC_BOILER_1.FinalPV_WFTX",
    250: "PLC_BOILER_1.FinalPV_SFTX",
    251: "PLC_BOILER_1.FinalPV_PTX",
    252: "PLC_BOILER_1.FinalPV_SHTTX",
    253: "PLC_BOILER_1.FinalPV_OGTTX",
    278: "PLC_BOILER_2.FinalPV_WLTX",
    279: "PLC_BOILER_2.FinalPV_WFTX",
    280: "PLC_BOILER_2.FinalPV_SFTX",
    281: "PLC_BOILER_2.FinalPV_PTX",
    283: "PLC_BOILER_2.FinalPV_OGTTX",
    292: "PLC_KLARIFIKASI.Suhu_CST1",
    293: "PLC_KLARIFIKASI.Suhu_CST2",
    294: "PLC_KLARIFIKASI.Suhu_CST3",
    304: "PLC_KLARIFIKASI.SludgeSeparator1_Hr",
    305: "PLC_KLARIFIKASI.SludgeSeparator1_Min",
    307: "PLC_KLARIFIKASI.SludgeSeparator2_Hr",
    308: "PLC_KLARIFIKASI.SludgeSeparator2_Min",
    310: "PLC_KLARIFIKASI.SludgeSeparator3_Hr",
    311: "PLC_KLARIFIKASI.SludgeSeparator3_Min",
    313: "PLC_KLARIFIKASI.SludgeSeparator4_Hr",
    314: "PLC_KLARIFIKASI.SludgeSeparator4_Min",
    317: "PLC_KLARIFIKASI.slugeSeparator5_Hr",
    318: "PLC_KLARIFIKASI.slugeseparatort5_min",
    320: "PLC_KLARIFIKASI.Ampere_OS5",
    322: "PLC_BOILER_2.FinalPV_SHTTX",
    364: "PLC_KLARIFIKASI.HRM_OS5",
    447: "PLC_KLARIFIKASI.PV_FM",
    448: "PLC_KLARIFIKASI.FM_Tot1",
    449: "PLC_KLARIFIKASI.FM_Tot2"
}

# for debugging purpose, the data will be output as a text file
# Function to log messages into a text file
def log_to_file(message):
    with open("websocket_data.txt", "a") as file:
        file.write(message + "\n")

# Function to filter out unwanted messages based on their pattern
def is_unwanted_message(message):
    # Patterns for messages you want to exclude
    pattern1 = re.compile(r'42\["return var to browser",\{"13":\d+\}\]')
    pattern2 = re.compile(r'42\["return var to browser",\{"15":"\d{2}:\d{2}:\d{2}"\}\]')
    pattern3 = re.compile(r'42\["return var to browser",\{"16":"\d+:\d{2}:\d{2}"\}\]')
    
    # Check if the message matches any of the unwanted patterns
    return pattern1.match(message) or pattern2.match(message) or pattern3.match(message)

# Function to extract and label data by id
def label_message_by_id(message):
    # Extract the data from the message
    match = re.search(r'42\["return var to browser",(\{.*?\})\]', message)  # Find the JSON-like object in the message
    if match:
        data = eval(match.group(1))  # Convert the string to a dictionary
        labeled_data = {}
        for key, value in data.items():
            key = int(key)  # Convert key to an integer
            if key in id_to_name:
                labeled_data[id_to_name[key]] = value
            else:
                labeled_data[f"Unknown ID {key}"] = value
        if labeled_data:
            return labeled_data
    return None  # If no valid data is found

# Function to handle incoming WebSocket messages
def on_message(ws, message):
    # Filter out unwanted messages
    if not is_unwanted_message(message):
        labeled_message = label_message_by_id(message)
        if labeled_message:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Get current timestamp
            formatted_message = f"Labeled Data at {timestamp}: {labeled_message}"
            print(f"Received: {formatted_message}")
            log_to_file(formatted_message)  # Log to the main log file
            
            # Create files for each labeled data entry
            for label, value in labeled_message.items():
                # Create a unique filename using the label
                filename = f"{label.replace('.', '_')}.txt"  # Replace '.' with '_' for the filename
                
                # Append timestamp and value to the individual file
                with open(filename, "a") as file:
                    file.write(f"{timestamp} - {label}: {value}\n")  # Append labeled data with timestamp

# Function to handle WebSocket errors
def on_error(ws, error):
    print(f"Error: {error}")
    log_to_file(f"Error: {error}")

# Function to handle WebSocket closure
def on_close(ws, close_status_code, close_msg):
    print("Connection closed")
    log_to_file("Connection closed")
    # Reconnect after a delay if the connection closes
    time.sleep(5)
    print("Reconnecting...")
    log_to_file("Reconnecting...")
    run_websocket()  # Try to reconnect

# Function to handle WebSocket opening
def on_open(ws):
    print("Connection opened")
    log_to_file("Connection opened")

# Function to run the WebSocket client
def run_websocket():
    websocket.enableTrace(True)
    # adolina
    # ws = websocket.WebSocketApp("wss://7111227700060158162.sg1.tunnel.iotbus.net/socket.io/?EIO=3&transport=websocket",
    #                             on_message=on_message,
    #                             on_error=on_error,
    #                             on_close=on_close,
    #                             on_open=on_open)
    # websocket.enableTrace(True)
    # ajamu
    ws = websocket.WebSocketApp("wss://7092627700120179018.hk1.tunnel.iotbus.net/socket.io/?EIO=3&transport=websocket",
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close,
                                on_open=on_open)

    # Run the WebSocket connection
    ws.run_forever()

if __name__ == "__main__":
    # Start the WebSocket client
    thread = threading.Thread(target=run_websocket)
    thread.start()

    # Keep the script running
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Script interrupted, closing connection")
        log_to_file("Script interrupted, closing connection")




# for production purpose, the data will be output to a database server
# # Function to log messages into a text file
# def log_to_file(message):
#     with open("websocket_data.txt", "a") as file:
#         file.write(message + "\n")

# # Function to filter out unwanted messages based on their pattern
# def is_unwanted_message(message):
#     pattern1 = re.compile(r'42\["return var to browser",\{"13":\d+\}\]')
#     pattern2 = re.compile(r'42\["return var to browser",\{"15":"\d{2}:\d{2}:\d{2}"\}\]')
#     pattern3 = re.compile(r'42\["return var to browser",\{"16":"\d+:\d{2}:\d{2}"\}\]')
    
#     return pattern1.match(message) or pattern2.match(message) or pattern3.match(message)

# # Function to extract and label data by id
# def label_message_by_id(message):
#     match = re.search(r'42\["return var to browser",(\{.*?\})\]', message)
#     if match:
#         data = eval(match.group(1))
#         labeled_data = {}
#         for key, value in data.items():
#             key = int(key)
#             if key in id_to_name:
#                 labeled_data[id_to_name[key]] = value
#             else:
#                 labeled_data[f"Unknown ID {key}"] = value
#         return labeled_data if labeled_data else None
#     return None

# # Database connection setup
# def create_db_connection():
#     connection_string = (
#         "Driver={SQL Server};"
#         "Server=38.47.80.152,1433;"
#         "Database=IOT_MILL;"
#         "UID=iot_mill_user_1;"
#         "PWD=i09c332s;"
#     )
#     return pyodbc.connect(connection_string)

# # Function to create a table if it does not exist
# def create_table(cursor, label):
#     table_name = label.replace('.', '_')  # Use '_' instead of '.' for table name
#     cursor.execute(f"""
#         IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{table_name}' AND xtype='U')
#         CREATE TABLE {table_name} (
#             Timestamp DATETIME,
#             Value NVARCHAR(255)
#         )
#     """)
#     cursor.commit()

# # Function to insert data into the table
# def insert_data(cursor, label, timestamp, value):
#     table_name = label.replace('.', '_')
#     cursor.execute(f"""
#         INSERT INTO {table_name} (Timestamp, Value)
#         VALUES (?, ?)
#     """, (timestamp, value))
#     cursor.commit()

# # Function to handle incoming WebSocket messages
# def on_message(ws, message):
#     if not is_unwanted_message(message):
#         labeled_message = label_message_by_id(message)
#         if labeled_message:
#             timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#             formatted_message = f"Labeled Data at {timestamp}: {labeled_message}"
#             print(f"Received: {formatted_message}")
#             log_to_file(formatted_message)

#             # Database operations
#             with create_db_connection() as conn:
#                 cursor = conn.cursor()
                
#                 for label, value in labeled_message.items():
#                     create_table(cursor, label)  # Create table if it doesn't exist
#                     insert_data(cursor, label, timestamp, value)  # Insert data into the table

# # Function to handle WebSocket errors
# def on_error(ws, error):
#     print(f"Error: {error}")
#     log_to_file(f"Error: {error}")

# # Function to handle WebSocket closure
# def on_close(ws, close_status_code, close_msg):
#     print("Connection closed")
#     log_to_file("Connection closed")
#     time.sleep(5)
#     print("Reconnecting...")
#     log_to_file("Reconnecting...")
#     run_websocket()

# # Function to handle WebSocket opening
# def on_open(ws):
#     print("Connection opened")
#     log_to_file("Connection opened")

# # Function to run the WebSocket client
# def run_websocket():
#     websocket.enableTrace(True)
#     ws = websocket.WebSocketApp("wss://7111227700060158162.sg1.tunnel.iotbus.net/socket.io/?EIO=3&transport=websocket",
#                                 on_message=on_message,
#                                 on_error=on_error,
#                                 on_close=on_close,
#                                 on_open=on_open)

#     ws.run_forever()

# if __name__ == "__main__":
#     thread = threading.Thread(target=run_websocket)
#     thread.start()

#     try:
#         while True:
#             time.sleep(1)
#     except KeyboardInterrupt:
#         print("Script interrupted, closing connection")
#         log_to_file("Script interrupted, closing connection")