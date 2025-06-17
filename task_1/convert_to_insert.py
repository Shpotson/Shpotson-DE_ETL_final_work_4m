import csv
import math

def bulk_insert(rows, filename, start_id):
    person_id = start_id
    with open(filename, "w", encoding="utf-8") as out1:
        out1.write('INSERT INTO personality (id, time_spent_alone, stage_fear, social_event_attendance, going_outside, drained_after_socializing, friends_circle_size, post_frequency, personality) VALUES\n')
        rows1 = []
        for row in rows:
            person_id += 1
            sql_row = (
                f'({person_id}, {row["Time_spent_Alone"]}, "{row["Stage_fear"]}", {row["Social_event_attendance"]}, {row["Going_outside"]}, '
                f'"{row["Drained_after_socializing"]}", {row["Friends_circle_size"]}, {row["Post_frequency"]}, "{row["Personality"]}")'
            )
            rows1.append(sql_row)
        out1.write(',\n'.join(rows1) + ';\n')

input_csv = "personality_datasert.csv"
output_sql1 = "insert_personality_bulk1.sql"
output_sql2 = "insert_personality_bulk2.sql"

with open(input_csv, encoding="utf-8") as f:
    reader = list(csv.DictReader(f))
    n = len(reader)
    half = math.ceil(n / 2)

    # Первая половина
    bulk_insert(reader[:half], output_sql1, 0)

    # Вторая половина
    bulk_insert(reader[half:], output_sql2, half + 1)

print("Созданы файлы insert_personality_bulk1.sql и insert_personality_bulk2.sql")