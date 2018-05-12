ssh using -L command. If someone else is using the port number of your choice, you might have to log out and choose another port number. In this example, I choose a port number 8976.

```bash
ssh -L 127.0.0.1:8976:127.0.0.1:8976 your_id@gw.hpc.nyu.edu
ssh -L 127.0.0.1:8976:127.0.0.1:8976 dumbo
```
```bash
export PYSPARK_DRIVER_PYTHON='/share/apps/python/3.4.4/bin/python'
export PYSPARK_DRIVER_PYTHON_OPTS='' 
module load python/3.4.4
module load spark/2.2.0
```

```bash
which python # should be /share/apps/python/3.4.4/bin/python
pyvenv myenv
source myenv/bin/activate # you need to run this particular line whenever you login
which python # should be ~/myenv/bin/python
pip install --upgrade pip
pip install jupyter
pip install findspark
pip install numpy
jupyter notebook --port=your_port_number
```

inside jupyter notebook, make sure to run these lines first before initializing spark or sc object
```Python
import findspark
findspark.init()
```
