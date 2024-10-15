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
    449: "PLC_KLARIFIKASI.FM_Tot2",
    376: "P_B1_TKM.WaterLevel_HL",
    377: "P_B1_TKM.WaterLevel_LL",
    378: "P_B1_TKM.WaterLevel_XL",
    380: "P_B1_TKM.Run_IDF",
    381: "P_B1_TKM.Run_FDF",
    382: "P_B1_TKM.Run_SecFan",
    389: "P_B1_TKM.Run_FWP2",
    390: "P_B1_TKM.FinalPV_WLTX",
    391: "P_B1_TKM.FinalPV_WFTX",
    392: "P_B1_TKM.FinalPV_SFTX",
    393: "P_B1_TKM.FinalPV_PTX",
    394: "P_B1_TKM.FinalPV_SHTTX",
    395: "P_B1_TKM.FinalPV_OGTTX",
    405: "P_BPV.Make_Up_AM",
    406: "P_BPV.Blow_Off_AM",
    407: "P_BPV.Tekanan_Boiler",
    408: "P_BPV.Tekanan_BPV",
    413: "P_BPV.Output_MUP",
    414: "P_BPV.Output_BOFF",
    419: "P_STR.S1_Inlet",
    420: "P_STR.S1_Exhaust",
    421: "P_STR.S1_Condenssate",
    422: "P_STR.S1_Mode",
    423: "P_STR.S1_Door",
    432: "P_STR.S1_Press",
    433: "P_STR.S1_Temp",
    434: "P_STR.S2_Inlet",
    435: "P_STR.S2_Exhaust",
    436: "P_STR.S2_Cond",
    437: "P_STR.S2_Mode",
    438: "P_STR.S2_Door",
    447: "P_STR.S2_Press",
    448: "P_STR.S2_Temp",
    449: "P_STR.S3_Inlet",
    450: "P_STR.S3_Exhaust",
    451: "P_STR.S3_Cond",
    452: "P_STR.S3_Mode",
    453: "P_STR.S3_Door",
    462: "P_STR.S3_Press",
    463: "P_STR.S3_Temp",
    467: "P_B2_MXTRM.WaterLevel_HL",
    468: "P_B2_MXTRM.WaterLevel_LL",
    469: "P_B2_MXTRM.WaterLevel_XL",
    471: "P_B2_MXTRM.Run_IDF",
    472: "P_B2_MXTRM.Run_FDF",
    473: "P_B2_MXTRM.Run_Fuel_Dist_Fan",
    474: "P_B2_MXTRM.Run_Feed_Water_Pump",
    480: "P_B2_MXTRM.FinalPV_WLTX",
    481: "P_B2_MXTRM.FinalPV_WFTX",
    482: "P_B2_MXTRM.FinalPV_SFTX",
    483: "P_B2_MXTRM.FinalPV_PTX",
    496: "P_PRS.Press1_Run",
    497: "P_PRS.Digester1_Run",
    498: "P_PRS.Press2_Run",
    499: "P_PRS.Digester2_Run",
    500: "P_PRS.Press3_Run",
    501: "P_PRS.Digester3_Run",
    502: "P_PRS.Press4_Run",
    503: "P_PRS.Digester4_Run",
    504: "P_PRS.Thresher1_Run",
    505: "P_PRS.Thresher2_Run",
    507: "P_PRS.Ampere_Press1",
    508: "P_PRS.Ampere_Digester1",
    509: "P_PRS.Ampere_Press2",
    510: "P_PRS.Ampere_Digester2",
    511: "P_PRS.Ampere_Press3",
    512: "P_PRS.Ampere_Digester3",
    513: "P_PRS.Ampere_Press4",
    514: "P_PRS.Ampere_Digester4",
    515: "P_PRS.Temp_Digester1",
    516: "P_PRS.Temp_Digester2",
    517: "P_PRS.Temp_Digester3",
    518: "P_PRS.Temp_Digester4",
    523: "P_PRS.Ampere_Thresher1",
    524: "P_PRS.Ampere_Thresher2",
    526: "P_PRS.Alarm_Status_D1",
    527: "P_PRS.Alarm_Status_D2",
    528: "P_PRS.Alarm_Status_D3",
    529: "P_PRS.Alarm_Status_D4",
    540: "P_KLFKS.Run_Decanter1",
    541: "P_KLFKS.Run_Decanter2",
    542: "P_KLFKS.Run_Vibro1",
    543: "P_KLFKS.Run_Vibro2",
    545: "P_KLFKS.Run_RO_Pump1",
    546: "P_KLFKS.Run_RO_Pump2",
    547: "P_KLFKS.Run_SolidConv1",
    548: "P_KLFKS.Run_SolidConv2",
    553: "P_KLFKS.Suhu_CST1",
    554: "P_KLFKS.Suhu_CST2",
    555: "P_KLFKS.Ampere_Decanter1",
    556: "P_KLFKS.Ampere_Decanter2",
    563: "P_KLFKS.PV_FM_DCTR1",
    579: "P_KLFKS.PV_FM_DCTR2",
    624: "P_BPV.PM_GCat_Daya",
    634: "P_BPV.PM_GCat_Frequency",
    637: "P_BPV.PM_T1_Daya",
    647: "P_BPV.PM_T1_Frequency",
    650: "P_BPV.PM_T2_Daya",
    660: "P_BPV.PM_T2_Frequency",
    663: "P_BPV.PM_GMan_Daya",
    673: "P_BPV.PM_GMan_Frequency",
    683: "P_BPV.Temp_Final_PV",
    686: "P_B1_TKM.Run_FWP_1",
    694: "P_PRS.Final_PV_Flow_Meter_Oil",
    699: "P_PRS.Oil_Flow_Tot1",
    700: "P_PRS.Oil_Flow_Tot2",
    711: "P_KLFKS.FM_DCNTR1_tot1",
    712: "P_KLFKS.FM_DCNTR1_tot2",
    713: "P_KLFKS.FM_DCNTR2_tot1",
    714: "P_KLFKS.FM_DCNTR2_tot2",
    723: "P_PRS.FinalOut_LVL_D1",
    724: "P_PRS.FinalOut_LVL_D2",
    725: "P_PRS.FinalOut_LVL_D3",
}

# Function to log messages into a text file
def log_to_file(message):
    with open("websocket_data.txt", "a") as file:
        file.write(message + "\n")

# Function to filter out unwanted messages based on their pattern
def is_unwanted_message(message):
    pattern1 = re.compile(r'42\["return var to browser",\{"13":\d+\}\]')
    pattern2 = re.compile(r'42\["return var to browser",\{"15":"\d{2}:\d{2}:\d{2}"\}\]')
    pattern3 = re.compile(r'42\["return var to browser",\{"16":"\d+:\d{2}:\d{2}"\}\]')
    
    return pattern1.match(message) or pattern2.match(message) or pattern3.match(message)

# Function to extract and label data by id
def label_message_by_id(message):
    match = re.search(r'42\["return var to browser",(\{.*?\})\]', message)
    if match:
        data = eval(match.group(1))
        labeled_data = {}
        for key, value in data.items():
            key = int(key)
            if key in id_to_name:
                labeled_data[id_to_name[key]] = value
            else:
                labeled_data[f"Unknown ID {key}"] = value
        return labeled_data if labeled_data else None
    return None

# Database connection setup
def create_db_connection():
    connection_string = (
        "Driver={SQL Server};"
        "Server=38.47.80.152,1433;" 
        "Database=IOT_MILL;"
        "UID=iot_mill_user_1;"
        "PWD=i09c332s;"
    )
    return pyodbc.connect(connection_string)

# Function to create a table if it does not exist, with an additional 'data_from' field
def create_table(cursor, label):
    table_name = label.replace('.', '_')  # Use '_' instead of '.' for table name
    cursor.execute(f"""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{table_name}' AND xtype='U')
        CREATE TABLE {table_name} (
            Timestamp DATETIME,
            Value NVARCHAR(255),
            Data_From NVARCHAR(255)  -- Add the new Data_From field
        )
    """)
    cursor.commit()


# Function to insert data into the table, with the additional 'data_from' field
def insert_data(cursor, label, timestamp, value, data_from):
    table_name = label.replace('.', '_')
    try:
        cursor.execute(f"""
            INSERT INTO {table_name} (Timestamp, Value, Data_From)
            VALUES (?, ?, ?)
        """, (timestamp, value, data_from))  # Insert data_from into the table
        cursor.commit()
        # Log success
        log_to_file(f"Successfully inserted data into {table_name}: Timestamp={timestamp}, Value={value}, Data_From={data_from}")
    except Exception as e:
        # Log failure
        log_to_file(f"Failed to insert data into {table_name}: Error={str(e)}")


# Function to handle incoming WebSocket messages
def on_message(ws, message, source):
    if not is_unwanted_message(message):
        labeled_message = label_message_by_id(message)
        if labeled_message:
            # Add source to labeled data
            labeled_message['source'] = source
            
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            formatted_message = f"Labeled Data at {timestamp}: {labeled_message}"
            print(f"Received: {formatted_message}")
            log_to_file(formatted_message)

            # Database operations
            with create_db_connection() as conn:
                cursor = conn.cursor()
                
                for label, value in labeled_message.items():
                    if label != "source":  # Skip the source field for database insertions
                        create_table(cursor, label)  # Create table if it doesn't exist
                        insert_data(cursor, label, timestamp, value, source)  # Insert data and source into the table


# Function to handle WebSocket errors
def on_error(ws, error):
    print(f"Error: {error}")
    log_to_file(f"Error: {error}")

# Function to handle WebSocket closure
def on_close(ws, close_status_code, close_msg):
    print("Connection closed")
    log_to_file("Connection closed")
    time.sleep(5)
    print("Reconnecting...")
    log_to_file("Reconnecting...")
    run_websocket_adolina()
    run_websocket_ajamu()

# Function to handle WebSocket opening
def on_open(ws):
    print("Connection opened")
    log_to_file("Connection opened")

# Function to run the WebSocket client for Adolina
def run_websocket_adolina():
    websocket.enableTrace(True)
    ws = websocket.WebSocketApp("wss://7111227700060158162.sg1.tunnel.iotbus.net/socket.io/?EIO=3&transport=websocket",
                                on_message=lambda ws, message: on_message(ws, message, "Adolina"),
                                on_error=on_error,
                                on_close=on_close,
                                on_open=on_open)
    ws.run_forever()

# Function to run the WebSocket client for Ajamu
def run_websocket_ajamu():
    websocket.enableTrace(True)
    ws = websocket.WebSocketApp("wss://7092627700120179018.hk1.tunnel.iotbus.net/socket.io/?EIO=3&transport=websocket",
                                on_message=lambda ws, message: on_message(ws, message, "Ajamu"),
                                on_error=on_error,
                                on_close=on_close,
                                on_open=on_open)
    ws.run_forever()

if __name__ == "__main__":
    # Run WebSocket for both Adolina and Ajamu in separate threads
    adolina_thread = threading.Thread(target=run_websocket_adolina)
    ajamu_thread = threading.Thread(target=run_websocket_ajamu)
    
    adolina_thread.start()
    ajamu_thread.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Script interrupted, closing connection")
        log_to_file("Script interrupted, closing connection")
