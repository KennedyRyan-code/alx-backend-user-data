#!/usr/bin/env python3
"""
Module for handling Personal Data
"""

from logging import LogRecord, StreamHandler
from typing import List
import re
import csv
import logging
import os
import mysql.connector


PII_FIELDS = ("name", "email", "ssn", "password")


def filter_datum(fields: List[str], redaction: str,
                 message: str, separator: str) -> str:
    """ Returns a log message obfuscated """
    for f in fields:
        msg = re.sub(f'{f}=.*?{separator}',
                     f'{f}={redaction}{separator}', message)
    return msg


def get_logger() -> logging.Logger:
    """Return a Logger Object"""
    logger = logging.getLogger("user_data")
    logger.setLevel(logging.INFO)
    logger.propagate = False

    handler = logging.StreamHandler()
    formatter = RedactingFormatter(fields=PII_FIELDS)
    handler.setFormatter(formatter)

    logger.addHandler(handler)
    return logger


def get_db() -> mysql.connector.connection.MySQLConnection:
    """Returns a connector to a MySQL database"""
    username = os.getenv("PERSONAL_DATA_DB_USERNAME", "root")
    password = os.getenv("PERSONAL_DATA_DB_PASSWORD", "")
    host = os.getenv("PERSONAL_DATA_DB_HOST", "localhost")
    db_name = os.getenv("PERSONAL_DATA_DB_NAME")

    return mysql.connector.connect(
            user=username,
            password=password,
            host=host,
            database=db_name)


class RedactingFormatter(logging.Formatter):
    """ Redacting Formatter class
    """

    REDACTION = "***"
    FORMAT = "[HOLBERTON] %(name)s %(levelname)s %(asctime)-15s: %(message)s"
    SEPARATOR = ";"

    def __init__(self, fields: List[str]):
        super(RedactingFormatter, self).__init__(self.FORMAT)

    def format(self, record: logging.LogRecord) -> str:
        """ Filters values in incoming log records using filter_datum """
        record.msg = filter_datum(self.fields, self.REDACTION,
                                  record.getMessage(), self.SEPARATOR)
        return super(RedactingFormatter, self).format(record)


def main():
    """
    obtain a database connection using get_db
    and retrieve all rows in the users table
    and display each row under a filtered format like this:
    """
    logger = logging.getLogger("user_data")
    logger.setLevel(logging.INFO)
    logger.propagate = False

    handler = logging.StreamHandler()
    formatter = RedactingFormatter(fields=("name", "email", "phone", "ssn", "password"))
    handler.setFormatter(formatter)

    logger.addHandler(handler)
    db = get_db()
    cursor = db.cursor(dictionary=True)
    cursor.execute("SELECT * FROM users;")
    for row in cursor:
        log_message = "; ".join(f"{key}={value}" for key, value in row.items())
        logger.info(log_message)

    cursor.close()
    db.close()


if __name__ == "__main__":
    main()
