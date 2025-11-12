import os, time, io, csv
import json
import datetime
import sys

import requests
import pyodbc
import hashlib
import math
import argparse


class Main:
    def __init__(self, server=None, database=None):
        # Initialize
        if server:
            self.server = server
        else:
            self.server = r'HI000090\SQLEXPRESS'
        if database:
            self.database = database
        else:
            self.database = 'KPI DB'
        self.connect()
        self.cursor = self.connection.cursor()
        self.cursor.fast_executemany = True
        api_token = os.getenv("KNOWBE4_TOKEN1")
        if not api_token:
            self._printLogregel("[ERROR] API-token ontbreekt. Stel KNOWBE4_TOKEN in als omgevingsvariabele.")
            sys.exit()
        self.page_size = 500
        self.headers = {
            "Authorization": f"Bearer {api_token}",
            "Accept": "application/json"
        }

        # Empty tables
        self.emptyTables()
        # Get users
        self.getUsers()
        time.sleep(10)
        # Get Risico scores
        self.getRiskScore()
        time.sleep(10)
        # Get phishing Campaigns
        self.getCampaigns()
        time.sleep(10)
        # Get responses
        self.getResponses()
        time.sleep(10)
        # Get Training campaigns
        self.getTrainingCampaigns()
        time.sleep(10)
        # Get Trainings Campaigns followed
        self.getTrainingCampaignsFollowed()
        # History update
        procedure = 'EXEC [dbo].[set_history]'
        self.cursor.execute(procedure)
        self.cursor.commit()
        self.cursor.close()
        return

    def getUsers(self):
        table = 'users'
        page = 1
        total_inserted = 0
        insert_sql = "INSERT INTO dbo.Users (id, ehash, phish_prone_percentage, status) VALUES (?, ?, ?, ?)"
        api_url = "https://eu.api.knowbe4.com/v1/users"
        while True:
            data = fetch_page(self.headers, api_url, page, self.page_size)
            # Bepaal items robuust
            if isinstance(data, list):
                items = data
            elif isinstance(data, dict):
                # Probeer gangbare keys, val terug op de eerste list in de dict
                items = data.get("data") or data.get("items") or data.get("users") or []
            else:
                items = []
            if not items:
                break
            batch = []
            for it in items:
                row = flatten(it, table)
                if row["id"] is None:
                    # Log 1x voorbeeld en sla over
                    self._printLogregel("[WARN] Item zonder id, sample:", json.dumps(it, ensure_ascii=False)[:200])
                    continue
                batch.append((row["id"], row["ehash"], row["phish_prone_percentage"], row["status"]))
            if batch:
                self.cursor.executemany(insert_sql, batch)
                total_inserted += len(batch)
            page += 1
            time.sleep(0.2)
        self.connection.commit()
        self._printLogregel(f"Inserted users: {total_inserted}")
        return

    def getRiskScore(self):
        table = 'user_risk'
        total_updated = 0
        # Ophalen alle actieve gebruikers (gekoppeld aan een unit)
        query = "SELECT id FROM [dbo].[Vw_Active_Users]"
        self.cursor.execute(query)
        data = self.cursor.fetchall()
        for item in data:
            api_url = f"https://eu.api.knowbe4.com/v1/users/{item[0]}"
            data = fetch_page(self.headers, api_url, 1, 1)
            row = flatten(data, table)
            update_sql = (f"UPDATE dbo.Users SET [current_risk_score] = {row["current_risk_score"]} WHERE "
                          f"[id]={row["id"]}")
            self.connection.execute(update_sql)
            total_updated += 1
            # Ophalen in batches van 10 calls om de API interface niet te overspoelen (error 429)
            if total_updated % 10 == 0:
                self.connection.commit()
                time.sleep(10)
            else:
                time.sleep(0.2)
        self.connection.commit()
        self._printLogregel(f"Updated users with risk score: {total_updated}")
        return

    def getCampaigns(self):
        table = 'phishing_tests'
        page = 1
        total_inserted = 0
        insert_sql = ("INSERT INTO dbo.Phishing_tests (campaign_id, pst_id, status, name, started_at, duration) "
                      "VALUES (?, ?, ?, ?, ?, ?)")
        api_url = "https://eu.api.knowbe4.com/v1/phishing/security_tests"
        while True:
            data = fetch_page(self.headers, api_url, page, self.page_size)
            # Bepaal items robuust
            if isinstance(data, list):
                items = data
            elif isinstance(data, dict):
                # Probeer gangbare keys, val terug op de eerste list in de dict
                items = data.get("data") or data.get("items") or data.get("users") or []
            else:
                items = []
            if not items:
                break
            batch = []
            for it in items:
                row = flatten(it, table)
                if row["campaign_id"] is None:
                    # Log 1x voorbeeld en sla over
                    self._printLogregel("[WARN] Item zonder id, sample:", json.dumps(it, ensure_ascii=False)[:200])
                    continue
                batch.append((row["campaign_id"], row["pst_id"], row["status"], row["name"],
                              row["started_at"], row["duration"]))
            if batch:
                self.cursor.executemany(insert_sql, batch)
                total_inserted += len(batch)
            page += 1
            time.sleep(0.2)
        self.connection.commit()
        self._printLogregel(f"Inserted phishing campaigns: {total_inserted}")

    def getTrainingCampaigns(self):
        table = 'campaigns'
        page = 1
        total_inserted = 0
        insert_sql = ("INSERT INTO dbo.training_campaigns (campaign_id, name, status, start_date) "
                      "VALUES (?, ?, ?, ?)")
        api_url = "https://eu.api.knowbe4.com/v1/training/campaigns"
        while True:
            data = fetch_page(self.headers, api_url, page, self.page_size)
            # Bepaal items robuust
            if isinstance(data, list):
                items = data
            elif isinstance(data, dict):
                # Probeer gangbare keys, val terug op de eerste list in de dict
                items = data.get("data") or data.get("items") or data.get("users") or []
            else:
                items = []
            if not items:
                break
            batch = []
            for it in items:
                row = flatten(it, table)
                if row["campaign_id"] is None:
                    # Log 1x voorbeeld en sla over
                    self._printLogregel("[WARN] Item zonder id, sample:", json.dumps(it, ensure_ascii=False)[:200])
                    continue
                batch.append((row["campaign_id"], row["name"], row["status"], row["start_date"]))
            if batch:
                self.cursor.executemany(insert_sql, batch)
                total_inserted += len(batch)
            page += 1
            time.sleep(0.2)
        self.connection.commit()
        self._printLogregel(f"Inserted training campaigns: {total_inserted}")
        return

    def getResponses(self):
        table = 'phishing_result'
        total_updated = 0
        api_calls = 0
        descriptions = None
        # Ophalen alle actieve gebruikers
        query = "SELECT [Id], 0 AS [Phishing_Send], 0 AS [Phishing_Responded] FROM [dbo].[Vw_Active_Users]"
        self.cursor.execute(query)
        data = self.cursor.fetchall()
        self.users = []
        descriptions = self.cursor.description
        for record in data:
            self.users.append(list(record))
        # Ophalen alle actieve phishing campagnes (In tabel Stam_Phishing_Campaigns: Active=1)
        query = "SELECT pst_id FROM [dbo].[Vw_Active_Phishing_Campaigns]"
        self.cursor.execute(query)
        data = self.cursor.fetchall()
        for pst_id in data:
            api_url = f"https://eu.api.knowbe4.com/v1/phishing/security_tests/{pst_id[0]}/recipients"
            page = 1
            while True:
                data = fetch_page(self.headers, api_url, page, self.page_size)
                # Bepaal items robuust
                if isinstance(data, list):
                    items = data
                elif isinstance(data, dict):
                    # Probeer gangbare keys, val terug op de eerste list in de dict
                    items = data.get("data") or data.get("items") or data.get("pst_id") or []
                else:
                    items = []
                if not items:
                    break
                for it in items:
                    row = flatten(it, table)
                    self._update_users(row["user"], row["delivered_at"], row["reported_at"])
                page += 1
                api_calls = api_calls + 1
                if api_calls % 10 == 0:
                    time.sleep(10)
                else:
                    time.sleep(0.2)
        for user in self.users:
            if user[1] > 0:
                percentage = math.ceil((user[2]/user[1]*100)*100)/100
                query = f"UPDATE [dbo].[users] SET [Percentage_Response] = {percentage} WHERE [id]={user[0]}"
                self.cursor.execute(query)
                total_updated += 1
        self.cursor.commit()
        self._printLogregel(f"Updated users with response percentage: {total_updated}")
        return

    def getTrainingCampaignsFollowed(self):
        table = 'Campagne_result'
        total_updated = 0
        api_calls = 0
        descriptions = None
        # Ophalen alle actieve gebruikers
        query = ("SELECT [Id], 0 AS [Campaigns_Enrolled], 0 AS [Campaigns_Finished], 0 AS [Poicies_Enrolled], "
                 "0 AS [Policies_Finished] FROM [dbo].[Vw_Active_Users]")
        self.cursor.execute(query)
        data = self.cursor.fetchall()
        self.users = []
        descriptions = self.cursor.description
        for record in data:
            self.users.append(list(record))
        # Ophalen alle actieve phishing campagnes (In tabel Stam_Phishing_Campaigns: Active=1)
        query = "SELECT campaign_id, [type] FROM [dbo].[Vw_Active_Training_Campaigns]"
        self.cursor.execute(query)
        data = self.cursor.fetchall()
        for campaign in data:
            trainings_type = campaign[1]
            api_url = f"https://eu.api.knowbe4.com/v1/training/enrollments?campaign_id={campaign[0]}"
            page = 1
            while True:
                data = fetch_page(self.headers, api_url, page, self.page_size)
                # Bepaal items robuust
                if isinstance(data, list):
                    items = data
                elif isinstance(data, dict):
                    # Probeer gangbare keys, val terug op de eerste list in de dict
                    items = data.get("data") or data.get("items") or data.get("pst_id") or []
                else:
                    items = []
                if not items:
                    break
                for it in items:
                    row = flatten(it, table)
                    if row["user"]:
                        self._update_users_training(row["user"], row["status"], trainings_type)
                page += 1
                api_calls = api_calls + 1
                if api_calls % 10 == 0:
                    time.sleep(10)
                else:
                    time.sleep(0.2)
        for user in self.users:
            if user[1] > 0:
                percentage_t = math.ceil((user[2]/user[1]*100)*100)/100
            else:
                percentage_t = 0
            if user[3] > 0:
                percentage_p = math.ceil((user[4] / user[3] * 100) * 100) / 100
            else:
                percentage_p = 0
            query = (f"UPDATE [dbo].[users] SET [Percentage_Viewed] = {percentage_t}, "
                     f"[percentage_Policies] = {percentage_p} WHERE [id]={user[0]}")
            self.cursor.execute(query)
            total_updated += 1

        self.cursor.commit()
        self._printLogregel(f"Updated all users with training percentage: {total_updated}")

        return

    def emptyTables(self):
        self.cursor.execute("DELETE FROM dbo.Users")
        self.cursor.execute("DELETE FROM dbo.Phishing_Tests")
        self.cursor.execute("DELETE FROM dbo.Training_Campaigns")
        return

    def connect(self):
        connection_string = ("DRIVER={ODBC Driver 17 for SQL Server};"
                             f"SERVER={self.server};"
                             f"DATABASE={self.database};"
                             "Trusted_Connection=yes;")
        self.connection = pyodbc.connect(connection_string)
        return

    def _update_users(self, usr, send=None, responded=None):
        usr_id = usr["id"]
        for user in self.users:
            if user[0] == usr_id:
                if send:
                    user[1] += 1
                if responded:
                    user[2] += 1
        return

    def _update_users_training(self, usr, status, trainings_type):
        usr_id = usr["id"]
        for user in self.users:
            if user[0] == usr_id:
                if trainings_type == 'T':
                    user[1] += 1
                    if status == 'Passed':
                        user[2] += 1
                elif trainings_type == 'P':
                    user[3] += 1
                    if status == 'Passed':
                        user[4] += 1
        return

    def _printLogregel(self, regel):
        timestamp = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')
        input_query = f"INSERT INTO [dbo].[LogData]([Timestamp], [Log regel]) VALUES('{timestamp}', '{regel}')"
        self.cursor.execute(input_query)
        self.cursor.commit()
        return

    def fetch_page(self, headers, api_url, page, per_page):
        resp = requests.get(api_url, headers=headers,
                            params={"page": page, "per_page": per_page, "status": "active"},
                            timeout=30, allow_redirects=False)
        ct = (resp.headers.get("Content-Type") or "").lower()
        if resp.status_code >= 400:
            self._printLogregel(f"HTTP {resp.status_code}: {resp.text[:200]}")
            sys.exit()
        if "application/json" not in ct:
            self._printLogregel(f"Geen JSON (Content-Type={ct})  url={resp.url}\nBody:\n{resp.text[:200]}")
            sys.exit()
        return resp.json()


def to_float_or_none(v):
    if v is None or v == "":
        return None
    try:
        return float(v)
    except Exception:
        return None


def flatten(item, table):
    # Pas dit aan op basis van echte payload keys
    if table == 'users':                                                            # Gebruikers
        # https://eu.api.knowbe4.com/v1/users
        email = item.get("email").lower()
        return {
            "id": item.get("id"),
            "ehash": hashlib.sha256(email.encode("utf-8")).digest(),
            "phish_prone_percentage": to_float_or_none(item.get("phish_prone_percentage")),
            "status": item.get("status"),
            "payload_json": json.dumps(item, ensure_ascii=False)
        }
    elif table == 'user_risk':                                                      # Risico score
        # https://eu.api.knowbe4.com/v1/users/{user_id}
        return {
            "id": item.get("id"),
            "current_risk_score": to_float_or_none(item.get("current_risk_score")),
            "payload_json": json.dumps(item, ensure_ascii=False)
        }
    elif table == 'phishing_tests':                                                 # Campagnes
        # https: // eu.api.knowbe4.com / v1 / phishing / security_tests
        return {
            "campaign_id": item.get("campaign_id"),
            "pst_id": item.get("pst_id"),
            "status": item.get("status"),
            "name": item.get("name"),
            "started_at": datetime.datetime.strptime(item.get("started_at")[:10], '%Y-%m-%d'),
            "duration": item.get("duration"),
            "payload_json": json.dumps(item, ensure_ascii=False)
        }
    elif table == 'phishing_result':                                                # Rapporteren
        # https://eu.api.knowbe4.com/v1/phishing/security_tests/{pst_id}/recipients
        return {
            "user": item.get("user"),
            "delivered_at": item.get("delivered_at"),
            "reported_at": item.get("reported_at"),
            "payload_json": json.dumps(item, ensure_ascii=False)
        }
    elif table == 'campaigns':                                                      # Campagnes
        # https://eu.api.knowbe4.com/v1/training/campaigns
        return {
            "campaign_id": item.get("campaign_id"),
            "name": item.get("name"),
            "status": item.get("status"),
            "start_date": datetime.datetime.strptime(item.get("start_date")[:10], '%Y-%m-%d'),
            "payload_json": json.dumps(item, ensure_ascii=False)
        }
    elif table == 'Campagne_result':                                                # Bekeken campagnes
        # https://eu.api.knowbe4.com/v1/training/enrollments campaign_id=[campaign_id]
        return {
            "user": item.get("user"),
            "status": item.get("status"),
            "payload_json": json.dumps(item, ensure_ascii=False)
        }


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Importeer data naar SQL Server.")
    parser.add_argument("--server", required=False, help="Naam van de SQL Server")
    parser.add_argument("--database", required=False, help="Naam van de database")
    args = parser.parse_args()
    Main(args.server, args.database)
