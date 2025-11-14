import sys, os, time
import json
import pyodbc
import argparse
import requests
import hashlib
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta


class Main:
    def __init__(self, server, database, token):
        # Initialize
        self.server = server
        self.database = database
        self.token = token
        self._connect()
        self._printLogregel("Start of the Knowbe4 staging process")
        self._getToken()
        self.total_fetches = 0
        self.sleep_interval_short = 0.2
        self.sleep_interval_long = 10
        self.batch_size = 10
        self._deleteStaging()
        self.users = list()
        self.phishing_campaigns = list()
        self.training_campaigns = list()
        self._getUsers()                    # Import users
        self._getUserDetails()              # Import riskscore
        self._getPhishingCampaigns()        # Import phishing campaigns
        self._getPhishingRecipients()       # Import phing action by user
        self._getTrainingCampaigns()        # Import training campaigns
        self._getTrainingEnrollment()       # Import enrolled users
        self._printLogregel(f"Totaal {self.total_fetches} aantal fetches")

    def _getUsers(self):
        self.table = 'users'
        self.page = 1
        self.page_size = 500
        total_inserted = 0
        insert_sql = "INSERT INTO STG.Stg_kb4_Users (user_id, eHash, phish_prone_pct, hash_row) VALUES (?, ?, ?, ?)"
        self.api_url = "https://eu.api.knowbe4.com/v1/users"
        while True:
            data = self._fetch_page()
            if isinstance(data, list):
                items = data
            else:
                items = list()
            batch = list()
            if not items:
                break
            for it in items:
                row = self._flatten(it)
                if row["id"] is None:
                    # Log 1x voorbeeld en sla over
                    self._printLogregel("[WARN] Item zonder id, sample:", json.dumps(it, ensure_ascii=False))
                    continue
                batch.append((row["id"], row["ehash"], row["phish_prone_percentage"], row["hash_row"]))
                self.users.append(row["id"])
            if batch:
                self.cursor.executemany(insert_sql, batch)
                total_inserted += len(batch)
            self.page += 1
            time.sleep(self.sleep_interval_short)
        self.connection.commit()
        self._printLogregel(f"Inserted users: {total_inserted}")
        time.sleep(self.sleep_interval_long)
        return

    def _getUserDetails(self):
        self.table = 'user_details'
        total_inserted = 0
        insert_sql = (f"INSERT INTO STG.Stg_kb4_User_Detail (user_id, current_risk_score, hash_row) "
                      f"VALUES (?, ?, ?)")
        batch = list()
        self.page = 1
        self.page_size = 1
        for user in self.users:
            self.api_url = f"https://eu.api.knowbe4.com/v1/users/{user}"
            data = self._fetch_page()
            if data:
                row = self._flatten(data)
                batch.append((row["id"], row["current_risk_score"], row["hash_row"]))
                total_inserted += 1
                # Ophalen in batches van X calls om de API interface niet te overspoelen (error 429)
                if total_inserted % self.batch_size == 0:
                    time.sleep(self.sleep_interval_long)
                else:
                    time.sleep(self.sleep_interval_short)
        if batch:
            self.cursor.executemany(insert_sql, batch)
            total_inserted += len(batch)
            self.connection.commit()
        self._printLogregel(f"Inserted users with risk score: {total_inserted}")
        return

    def _getPhishingCampaigns(self):
        self.table = 'phishing_campaigns'
        self.page = 1
        self.page_size = 500
        total_inserted = 0
        insert_sql = ("INSERT INTO STG.Stg_kb4_Pst (campaign_id, pst_id, [status], name, started_at, "
                      "duration_days, hash_row) VALUES (?, ?, ?, ?, ?, ?, ?)")
        self.api_url = "https://eu.api.knowbe4.com/v1/phishing/security_tests"
        while True:
            data = self._fetch_page()
            if isinstance(data, list):
                items = data
            else:
                items = list()
            batch = list()
            if not items:
                break
            for it in items:
                row = self._flatten(it)
                if row["campaign_id"] is None:
                    # Log 1x voorbeeld en sla over
                    self._printLogregel("[WARN] Item zonder id, sample:", json.dumps(it, ensure_ascii=False))
                    continue
                # compare current_date with started_at + duration + 2 Years
                finish_date = row["started_at"] + relativedelta(years=2) + timedelta(days=100)
                if datetime.today() > finish_date:
                    batch.append((row["campaign_id"], row["pst_id"], row["status"], row["name"],
                                  row["started_at"], row["duration"], row["hash_row"]))
                    self.phishing_campaigns.append(row["pst_id"])
            if batch:
                self.cursor.executemany(insert_sql, batch)
                total_inserted += len(batch)
            self.page += 1
            time.sleep(self.sleep_interval_short)
        self.connection.commit()
        self._printLogregel(f"Inserted phishing campaigns: {total_inserted}")
        time.sleep(self.sleep_interval_long)
        return

    def _getPhishingRecipients(self):
        self.table = 'phishing_recipients'
        total_inserted = 0
        insert_sql = (f"INSERT INTO stg.Stg_kb4_Pst_Recipient(pst_id, [user_id], delivered_at, "
                      f"opened_at, clicked_at, replied_at, attachment_opened_at, macro_enabled_at, data_entered_at, "
                      f"qr_code_scanned_at, reported_at, hash_row) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
        batch = list()
        self.page = 1
        self.page_size = 500
        for campaign in self.phishing_campaigns:
            self.api_url = f"https://eu.api.knowbe4.com/v1/phishing/security_tests/{campaign}/recipients"
            self.page = 1
            while True:
                data = self._fetch_page()
                if isinstance(data, list):
                    items = data
                else:
                    items = list()
                if not items:
                    break
                for it in items:
                    row = self._flatten(it)
                    batch.append((row["pst_id"], row["user_id"], row["delivered_at"],
                                  row["opened_at"], row["clicked_at"], row["replied_at"], row["attachment_opened_at"],
                                  row["macro_enabled_at"], row["data_entered_at"], row["qr_code_scanned"],
                                  row["reported_at"], row["hash_row"]))
                    total_inserted += 1
                time.sleep(self.sleep_interval_short)
                self.page += 1
        if batch:
            self.cursor.executemany(insert_sql, batch)
            total_inserted += len(batch)
            self.connection.commit()
        self._printLogregel(f"Inserted users for phishing campaigns: {total_inserted}")
        return

    def _getTrainingCampaigns(self):
        self.table = 'training_campaigns'
        self.page = 1
        self.page_size = 500
        total_inserted = 0
        insert_sql = ("INSERT INTO STG.Stg_kb4_Training_Campaign (campaign_id, name, [status], start_date, hash_row) "
                      "VALUES (?, ?, ?, ?, ?)")
        self.api_url = "https://eu.api.knowbe4.com/v1/training/campaigns"
        while True:
            data = self._fetch_page()
            if isinstance(data, list):
                items = data
            else:
                items = list()
            batch = list()
            if not items:
                break
            for it in items:
                row = self._flatten(it)
                if row["campaign_id"] is None:
                    # Log 1x voorbeeld en sla over
                    self._printLogregel("[WARN] Item zonder id, sample:", json.dumps(it, ensure_ascii=False))
                    continue
                if row["status"] == "In Progress":
                    batch.append((row["campaign_id"], row["name"], row["status"], row["start_date"], row["hash_row"]))
                    self.training_campaigns.append(row["campaign_id"])
            if batch:
                self.cursor.executemany(insert_sql, batch)
                total_inserted += len(batch)
            self.page += 1
            time.sleep(self.sleep_interval_short)
        self.connection.commit()
        self._printLogregel(f"Inserted training campaigns: {total_inserted}")
        time.sleep(self.sleep_interval_long)
        return

    def _getTrainingEnrollment(self):
        self.table = 'training_enrollment'
        total_inserted = 0
        insert_sql = (f"INSERT INTO stg.Stg_kb4_training_enrollment(enrollment_id, campaign_id, user_id, "
                      f"enrollment_date, status, hash_row) VALUES(?, ?, ?, ?, ?, ?)")
        batch = list()
        self.page = 1
        self.page_size = 500
        for campaign in self.training_campaigns:
            self.api_url = f"https://eu.api.knowbe4.com/v1/training/enrollments?campaign_id={campaign}"
            self.page = 1
            while True:
                data = self._fetch_page()
                if isinstance(data, list):
                    items = data
                else:
                    items = list()
                if not items:
                    break
                for it in items:
                    row = self._flatten(it)
                    batch.append((row["enrollment_id"], campaign, row["user_id"], row["enrollment_date"],
                                  row["status"], row["hash_row"]))
                    total_inserted += 1
                time.sleep(self.sleep_interval_short)
                self.page += 1
        if batch:
            self.cursor.executemany(insert_sql, batch)
            total_inserted += len(batch)
            self.connection.commit()
        self._printLogregel(f"Inserted users for training campaigns: {total_inserted}")
        return

    def _flatten(self, item):
        if self.table == 'users':
            email = item.get("email").lower()
            ehash = hashlib.sha256(email.encode("utf-8")).digest()
            user_id = item.get("id")
            ppp = to_float_or_none(item.get("phish_prone_percentage"))
            total = (str(user_id) + str(email) + str(ppp))
            hashrow = hashlib.sha256(total.encode("utf-8")).digest()
            return {"id": user_id, "ehash": ehash, "phish_prone_percentage": ppp, "hash_row": hashrow}
        elif self.table == 'user_details':
            user_id = item.get("id")
            crs = to_float_or_none(item.get("current_risk_score"))
            total = str(user_id) + str(crs)
            hashrow = hashlib.sha256(total.encode("utf-8")).digest()
            return {"id": user_id, "current_risk_score": crs, "hash_row": hashrow}
        elif self.table == 'phishing_campaigns':
            campaign_id = item.get("campaign_id")
            pst_id = item.get("pst_id")
            status = item.get("status")
            name = item.get("name")
            started_at = to_date_or_none(item.get("started_at"))
            duration = item.get("duration")
            total = str(campaign_id) + str(pst_id) + str(name) + str(started_at) + str(duration)
            hashrow = hashlib.sha256(total.encode("utf-8")).digest()
            return {"campaign_id": campaign_id, "pst_id": pst_id, "status": status, "name": name,
                    "started_at": started_at, "duration": duration, "hash_row": hashrow}
        elif self.table == 'phishing_recipients':
            pst_id = item.get("pst_id")
            user = item.get("user")
            user_id = user.get("id")
            delivered_at = to_date_or_none(item.get("delivered_at"))
            opened_at = to_date_or_none(item.get("opened_at"))
            clicked_at = to_date_or_none(item.get("clicked_at"))
            replied_at = to_date_or_none(item.get("replied_at"))
            attachment_opened_at = to_date_or_none(item.get("attachment_opened_at"))
            macro_enabled_at = to_date_or_none(item.get("macro_enabled_at"))
            data_entered_at = to_date_or_none(item.get("data_entered_at"))
            qr_code_scanned = to_date_or_none(item.get("qr_code_scanned"))
            reported_at = to_date_or_none(item.get("reported_at"))
            total = (str(pst_id) + str(user_id) + str(delivered_at) + str(opened_at) +
                     str(clicked_at) + str(replied_at) + str(attachment_opened_at) + str(macro_enabled_at) +
                     str(data_entered_at) + str(qr_code_scanned) + str(reported_at))
            hashrow = hashlib.sha256(total.encode("utf-8")).digest()
            return {"pst_id": pst_id, "user_id": user_id, "delivered_at": delivered_at,
                    "opened_at": opened_at, "clicked_at": clicked_at, "replied_at": replied_at,
                    "attachment_opened_at": attachment_opened_at, "macro_enabled_at": macro_enabled_at,
                    "data_entered_at": data_entered_at, "qr_code_scanned": qr_code_scanned,
                    "reported_at": reported_at, "hash_row": hashrow}
        elif self.table == "training_campaigns":
            campaign_id = item.get('campaign_id')
            name = item.get("name")
            status = item.get("status")
            start_date = to_date_or_none(item.get("start_date"))
            total = str(campaign_id) + str(name) + str(status) + str(start_date)
            hashrow = hashlib.sha256(total.encode("utf-8")).digest()
            return {"campaign_id": campaign_id, "name": name, "status": status, "start_date": start_date,
                    "hash_row": hashrow}
        elif self.table == "training_enrollment":
            enrollment_id = item.get("enrollment_id")
            user = item.get("user")
            user_id = user.get("id")
            enrollment_date = to_date_or_none(item.get("enrollment_date"))
            status = item.get("status")
            total = str(enrollment_id) + str(user_id) + str(enrollment_date) + str(status)
            hashrow = hashlib.sha256(total.encode("utf-8")).digest()
            return {"enrollment_id": enrollment_id, "user_id": user_id, "enrollment_date": enrollment_date,
                    "status": status, "hash_row": hashrow}

    def _deleteStaging(self):
        self.cursor.execute("DELETE FROM [STG].[Stg_kb4_Users]")
        self.cursor.execute("DELETE FROM [STG].[Stg_kb4_User_Detail]")
        self.cursor.execute("DELETE FROM [STG].[Stg_kb4_Pst]")
        self.cursor.execute("DELETE FROM [STG].[Stg_kb4_Pst_Recipient]")
        self.cursor.execute("DELETE FROM [STG].[Stg_kb4_Training_Campaign]")
        self.cursor.execute("DELETE FROM [STG].[Stg_kb4_Training_Enrollment]")
        return

    def _connect(self):
        if not self.server:
            self.server = r'HI000090\SQLEXPRESS'
        if not self.database:
            self.database = 'KPI database'
        connection_string = ("DRIVER={ODBC Driver 17 for SQL Server};"
                             f"SERVER={self.server};"
                             f"DATABASE={self.database};"
                             "Trusted_Connection=yes;")
        self.connection = pyodbc.connect(connection_string)
        self.cursor = self.connection.cursor()
        self.cursor.fast_executemany = True
        return

    def _getToken(self):
        if not self.token:
            self.token = 'KNOWBE4_TOKEN'
        api_token = os.getenv(self.token)
        if not api_token:
            self._printLogregel(f"[ERROR] API-token ontbreekt. Stel {self.token} in als omgevingsvariabele.")
            sys.exit()
        self.headers = {
            "Authorization": f"Bearer {api_token}",
            "Accept": "application/json"
        }

    def _printLogregel(self, regel):
        timestamp = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
        time.sleep(1)
        input_query = f"INSERT INTO [MST].[LogData]([Timestamp], [Log regel]) VALUES('{timestamp}', '{regel}')"
        self.cursor.execute(input_query)
        self.cursor.commit()
        return

    def _fetch_page(self):
        resp = requests.get(self.api_url, headers=self.headers,
                            params={"page": self.page, "per_page": self.page_size, "status": "active"},
                            timeout=30, allow_redirects=False)
        ct = (resp.headers.get("Content-Type") or "").lower()
        self.total_fetches += 1
        if resp.status_code >= 400:
            self._printLogregel(f"HTTP {resp.status_code}: {resp.text}")
            sys.exit()
        if "application/json" not in ct:
            self._printLogregel(f"Geen JSON (Content-Type={ct})  url={resp.url}\nBody:\n{resp.text}")
            sys.exit()
        return resp.json()


def to_float_or_none(v):
    if v is None or v == "":
        return None
    try:
        return float(v)
    except Exception:
        return None


def to_date_or_none(v):
    if v is None or v == "":
        return None
    try:
        return datetime.strptime(v[:10], '%Y-%m-%d')
    except Exception:
        return None


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Importeer data naar SQL Server.")
    parser.add_argument("--server", required=False, help="Naam van de SQL Server")
    parser.add_argument("--database", required=False, help="Naam van de database")
    parser.add_argument("--token", required=False, help="Naam van het API_token")
    args = parser.parse_args()
    Main(args.server, args.database, args.token)
