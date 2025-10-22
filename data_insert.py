import random
import mysql.connector
from faker import Faker
from mysql.connector import errorcode
from datetime import timezone

# --- Налаштування ---
TOTAL_PLAYERS = 1_000_000
TOTAL_MATCHES = 1_000_000
TOTAL_MATCH_RESULTS = 1_000_000  # Найбільша таблиця "фактів"
BATCH_SIZE = 50_000  # Розмір пакету для вставки


# ---------------------

def insert_players(cursor, fake: Faker, total: int, batch: int):
    """Генерує та вставляє гравців."""
    print(f"Generating {total} players...")
    sql = """
        INSERT INTO players (username, country_code, registration_date)
        VALUES (%s, %s, %s)
    """
    country_codes = ['US', 'UA', 'PL', 'DE', 'GB', 'CA', 'FR']

    for i in range(0, total, batch):
        batch_data = []
        current_batch_size = min(batch, total - i)
        for _ in range(current_batch_size):
            batch_data.append((
                fake.unique.user_name(),  # .unique гарантує унікальність
                random.choice(country_codes),
                fake.date_between(start_date='-5y', end_date='today')
            ))

        cursor.executemany(sql, batch_data)
        print(f"  Inserted {min(i + current_batch_size, total)} / {total} players")
    print("✅ Players insertion complete.\n")


def insert_matches(cursor, fake: Faker, total: int, batch: int):
    """Генерує та вставляє матчі."""
    print(f"Generating {total} matches...")
    sql = """
        INSERT INTO matches (game_mode, map_name, match_start, match_duration_seconds)
        VALUES (%s, %s, %s, %s)
    """
    game_modes = ['Deathmatch', 'Capture the Flag', 'Battle Royale', 'Team Deathmatch']
    map_names = ['Dust2', 'Nuketown', 'Olympus', 'Crossfire', 'Erangel', 'Blood Gulch']

    for i in range(0, total, batch):
        batch_data = []
        current_batch_size = min(batch, total - i)
        for _ in range(current_batch_size):
            batch_data.append((
                random.choice(game_modes),
                random.choice(map_names),

                # ❗️ ВАЖЛИВА ЗМІНА ТУТ:
                fake.date_time_between(start_date='-1y', end_date='now', tzinfo=timezone.utc),

                random.randint(600, 1800)  # Тривалість
            ))

        cursor.executemany(sql, batch_data)
        print(f"  Inserted {min(i + current_batch_size, total)} / {total} matches")
    print("✅ Matches insertion complete.\n")

def insert_match_results(cursor, total: int, batch: int, max_player_id: int, max_match_id: int):
    """Генерує та вставляє результати матчів, поєднуючи гравців та матчі."""
    print(f"Generating {total} match results...")
    sql = """
        INSERT INTO match_results (match_id, player_id, team, kills, deaths, score)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    teams = ['Red', 'Blue']

    for i in range(0, total, batch):
        batch_data = []
        current_batch_size = min(batch, total - i)
        for _ in range(current_batch_size):
            kills = random.randint(0, 35)
            deaths = random.randint(0, 30)
            score = (kills * 10) - (deaths * 5) + random.randint(0, 500)

            batch_data.append((
                random.randint(1, max_match_id),  # Випадковий ID матчу
                random.randint(1, max_player_id),  # Випадковий ID гравця
                random.choice(teams),
                kills,
                deaths,
                max(0, score)  # Рахунок не може бути негативним
            ))

        cursor.executemany(sql, batch_data)
        print(f"  Inserted {min(i + current_batch_size, total)} / {total} match results")
    print("✅ Match results insertion complete.\n")


def main():
    """Головна функція для підключення та запуску вставки."""
    connection = None
    try:
        connection = mysql.connector.connect(
            host="127.0.0.1",
            user="root",
            password="MySQL_Student123",  # ВАШ ПАРОЛЬ
            database="A04_Gaming"  # Назва БД з SQL-скрипту
        )
        connection.autocommit = False  # Керуємо транзакціями вручну
        cursor = connection.cursor()
        cursor.execute("SET time_zone = '+00:00';")
        fake = Faker()

        # 1. Вставляємо гравців
        insert_players(cursor, fake, TOTAL_PLAYERS, BATCH_SIZE)
        connection.commit()  # Зберігаємо зміни

        # 2. Вставляємо матчі
        insert_matches(cursor, fake, TOTAL_MATCHES, BATCH_SIZE)
        connection.commit()

        # 3. Вставляємо результати матчів
        # Припускаємо, що ID йдуть від 1 до TOTAL_...
        insert_match_results(cursor, TOTAL_MATCH_RESULTS, BATCH_SIZE, TOTAL_PLAYERS, TOTAL_MATCHES)
        connection.commit()

        print("🎉 All data successfully inserted into A04_Gaming!")

    except mysql.connector.Error as err:
        if connection:
            print("Rolling back changes due to error...")
            connection.rollback()  # Відкочуємо зміни у разі помилки
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print(f"Database 'A04_Gaming' does not exist")
        else:
            print(err)
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed.")


if __name__ == "__main__":
    main()