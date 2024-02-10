#!/usr/bin/env python3
"""
Main file
"""

filter_datum = __import__('filtered_logger').filter_datum
hash_password = __import__('encrypt_password').hash_password
is_valid = __import__('encrypt_password').is_valid

fields = ["password", "date_of_birth"]
messages = ["name=egg;email=eggmin@eggsample.com;password=eggcellent;date_of_birth=12/12/1986;", "name=bob;email=bob@dylan.com;password=bobbycool;date_of_birth=03/04/1993;"]

for message in messages:
    print(filter_datum(fields, 'xxx', message, ';'))

password = "MyAmazingPassw0rd"
print(hash_password(password))
print(hash_password(password))

encrypted_password = hash_password(password)
print(encrypted_password)
print(is_valid(encrypted_password, password))

