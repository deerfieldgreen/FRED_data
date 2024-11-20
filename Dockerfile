FROM python:3.9-slim

# Install git
RUN apt-get update && apt-get install -y git

# Set the Git user configuration
RUN git config --global user.email "kevin.stoll@deerfieldgreen.com"
RUN git config --global user.name "Kevin Stoll"

# Set git to not rebase on pull
RUN git config --global pull.rebase false

WORKDIR /app

COPY . /app

RUN pip install -r requirements.txt  # if you have dependencies

CMD ["python", "main.py"]
