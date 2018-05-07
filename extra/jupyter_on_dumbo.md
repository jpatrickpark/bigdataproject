1st step: We need to install jupyter. In order to do that, I installed anaconda python. (Maybe using virtualenv is better, but I did not make it work.)

```bash
wget https://repo.continuum.io/archive/Anaconda3-5.1.0-Linux-x86_64.sh
bash Anaconda3-5.1.0-Linux-x86_64.sh
```

answer 'yes' when the script asks "add path to .bashrc"

Check if the following line is added to ~/.bashrc file: 

```bash
export PATH="/home/your_id/anaconda3/bin:$PATH"
```

log out form dumbo completely

ssh using -L command. If someone else is using the port number of your choice, you might have to log out and choose another port number. In this example, I choose a port number 8976.

```bash
ssh -L 127.0.0.1:8976:127.0.0.1:8976 your_id@gw.hpc.nyu.edu
ssh -L 127.0.0.1:8976:127.0.0.1:8976 dumbo
```

Type these two lines (Or you can add these lines in ~/.bashrc so that these lines will be loaded when you log in. However, the port number must match the number you used when ssh-ing.)

```bash
export PYSPARK_DRIVER_PYTHON=jupyter
export PYSPARK_DRIVER_PYTHON_OPTS='notebook --port=8976'
```

If you just added the above two lines in ~/.bashrc, make sure to run source ~/.bashrc or log out and log back in

And run jupyter notebook with the following command:

```bash
pyspark
```

And copy the link which appeared, and paste it in the browser. Even though the link says localhost (dumbo), you should be able to access it in your local machine because you forwarded the port.

If syntax highlighting is not shown, just wait a minute for python kernel to be attached before the syntax highlighting shows up. The icon on your browser should be changed form hourglass to notebook shape.
