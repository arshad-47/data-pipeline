FROM flink:1.13.2-scala_2.12-java11

# Install dependencies
RUN apt-get update && \
    apt-get install -y curl unzip jq && \
    rm -rf /var/lib/apt/lists/*

# Install AWS CLI v2
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && \
    unzip awscliv2.zip && \
    ./aws/install && \
    rm -rf aws awscliv2.zip
