# Set the home directory to /home/ssm-user and navigate to it.
$HOME = /home/ssm-user
cd $HOME

# Generate a 2048-bit RSA private key and convert it to PKCS#8 format and save to rsa_key.p8
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8


# Extract the public key from the private key file (rsa_key.p8) and save to rsa_key.pub.
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub

# Then, extract part of the public key file and print the resulting single-line public key to a file named pub.Key.
grep -v KEY rsa_key.pub | tr -d '\n' | awk '{print $1}' > pub.Key

cat pub.Key