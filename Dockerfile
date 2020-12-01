FROM python:3.8

# Create app directory
WORKDIR /app

# Install app dependencies
COPY ./requirements.txt ./

RUN pip install -r requirements.txt

# Bundle app source
COPY ./src /app

RUN ls
RUN ls

EXPOSE 13800
CMD [ "python3", "main.py" ]