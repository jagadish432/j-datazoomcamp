# 1
## Added by Google
ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQDUzpcqW/PJJMW2c5JhhdfhlqBDMtMvo8/9/VkzouN3tLm5XuOia+DFcBVPE3KaxBGWSIoteUJsZpsGJSw2gCr0fEXS95TrrRwSmiI05OsimjpJdQfmJGFL9cVsjtClXBmEzzX60JtJr1mkytpf9noFV15w2bGOPg2GLAqXMQHZUhLsjUA0aXg0Jitpj+rQNS3CrcLnzvyyhWsACNeh2XmMaLHdm/8Xax02FmtKAUyERnMOSrvWxrzcAJClSqoAsjii4wICLd3K0HUcZvXFmgr20B0A4u3m2yOpV+hBBwt7UWabTpixYdDEV0LD0/ofoSINAoNHjpifqprE/o2oJLXn jagadish

### the above ^^ ssh line was in ~/.ssh/authorized_keys in the gcp vm, probably it's automatically added by gcp

# 2
## my personal laptop ssh config to ssh into the gcp vm
Host de-zoomcamp
    HostName <instance public IP>
    User jagadish
    IdentityFile C:/Users/jagadish/.ssh/gcp
    LocalForward 127.0.0.1:5432 127.0.0.1:5432
    LocalForward 127.0.0.1:8080 127.0.0.1:8080
    LocalForward 127.0.0.1:4200 127.0.0.1:4200
    LocalForward 127.0.0.1:4040 127.0.0.1:4040

### this ssh file should be in our personal/working laptop in ~/users/<loggedin_username>/.ssh/ folder and this folder should also have ssh public and private key files which we used while creating the VM instance in the cloud
