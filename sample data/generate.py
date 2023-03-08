from faker import Faker
import random

fake = Faker()
fake.seed()

print ("Name,Email,Job,Phone,Age");

for _ in range(5000):
	dataLine = '' + fake.name() + '|' + fake.email() + '|' + fake.job() + '|' + fake.phone_number() + '|' + str(random.randint(18,80));
	print (dataLine);