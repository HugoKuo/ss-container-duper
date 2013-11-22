How to use the simple container-duper 

[Install] 
	1. Download the tarball 
	2. Change to root
	3. Copy the tarball to /opt
	4. Extract it $> tar zxvf $tarball_name
	5. $> cd /opt/ss-container-duper
	6. change mode : $> chmod 777 ss-container-duper.py


[Preparation]
	1. Making directory /etc/ss-container-duper
			$> sudo mkdir /etc/ss-container-duper

	2. Preparing the conf file in /etc/ss-container-duper/ss-container-duper.conf , please check the example. 
			$> sudo cp ss-container-duper.conf /etc/ss-container-duper/ss-container-duper.conf

	3. Touch a the log file in /var/log/swift/, name it as ss-container-duper.log
			$> sudo touch /var/log/swift/ss-container-duper.log

[Running]
	* Direct to execute the script with root permission
			$> python /opt/ss-container-duper/ss-container-duper.py

	* Run as a Cronjob 
			$> crontab -e 
				Add the following line and save it: 
				 * * * * * /opt/ss-container-duper/ss-container-duper.py

	* Trigger it by other event(not ready yet)


[Watching Log]
	$> tailf /var/log/swift/ss-container-duper.log



[To tracking new account]
	If you want to track new account, you have to :
	1. Stop the cronjob
	2. Delete the record-pickle.db in /etc/ss-container-duper
	3. Modify the configuration for another account
	4. Start cronjob
