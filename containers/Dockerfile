# parent image
FROM networkstatic/iperf3

# install needed packages
RUN apt-get update -y && apt-get upgrade -y && apt-get install -y \
    net-tools \
    iputils-ping \
    iproute2 \
    telnet telnetd
    
# run bash interpreter
CMD /bin/bash
