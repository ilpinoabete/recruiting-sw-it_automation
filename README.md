# Introduction to the assignment

Given the assigned tasks i decided to proceed with the first one, whose instructions asked me to ssh into a given ubuntu server VM, install docker server on it and spin up a NocoDB container.
After that i was requested to create a python script that was able to receive a JSON stream and parse and push them into the database thanks to the REST APIs. It was also requested to create a Telegram BOT that was able to retrieve certain data and add new user to the hosted database.

# Docker and database setup
## Docker installation
First of all i made sure the server was up to date running the command `sudo apt update && sudo apt upgrade -y` I than installed the editor `micro` to edit the configuration files.
After that i could install docker trough the official script available in the [official documentation](https://docs.docker.com/engine/install/ubuntu/) that i downloaded with ``curl -fsSL https://get.docker.com -o get-docker.sh`` and executed launching the follow command `sudo chmod +x get-docker.sh && sudo ./get-docker.sh`.

After verifying that docker was actually installed in the system and doing the same for python i could carry on with the task
```bash
user@recruiting-hr1:~$ docker -v
Docker version 28.4.0, build d8eb465

user@recruiting-hr1:~$ which python3
/usr/bin/python3
```

## NocoDB installation
The second task in order of business was to install NocoDB in the VM, to do so i decided to use docker compose as to as make the maintenance of the future whole application stack easier.
Following the [official NocoDB docker cocumentation](https://nocodb.com/docs/self-hosting/installation/docker) i pulled the official NocoDB repository via the command 
`git clone https://github.com/nocodb/nocodb` i than moved to the `nocodb/docker-compose/2_pg` directory and i slightly modified the default compose with these goals:
* Remove postgres sensible information (user, password, and database name) frome the file to use a .env
* Create a custom network to make it easier to group the stack's container

The `docker-compose.yaml` as well as the `.env` files can be found in this repository as all the other files whose upload was requested by the task.

Once the setup was completed i could run the `sudo docker compose up -d` command and, as expected, the NocoDB webUI was reachable at the `http://serverLocalIP:8080` address.
I successfully logged into the NocoDB webUI and them I could create the Users, Departments and Areas tables and started working on the two python scripts.

# Telegram bot
## Used libraries
As requested by the task, to create the bot i used the `python-telegram-bot` library referring to [it's official documentation](https://docs.python-telegram-bot.org/en/stable/index.html)
Other libraries were particularly handful such as:
* [python-dotenv](https://pypi.org/project/python-dotenv/) and [os](https://docs.python.org/3/library/os.html) to load the environment variables
* [io](https://docs.python.org/3/library/io.html) and [csv](https://docs.python.org/3/library/csv.html) to create a temporary CSV file directly in the memory without saving it on any disk
* [requests](https://pypi.org/project/requests/) to interact with the NocoDB APIs

Note that thanks to the `requirements.txt` all the python dependencies can be installed simply running `pip install -r requirements.txt`
## Bot usage
The python bot has only two commands: `/start` and `/list`, the first one welcomes the user while the second one lists to the user the tables present on the database and returns a csv file containing the selected table's data.
To function correctly the bot requires some environment variables that can be found in the .env file
