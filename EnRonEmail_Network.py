import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import re
from DateTime import DateTime
from datetime import datetime
import datetime
import operator
import time
from itertools import groupby
import itertools
from scipy.stats.distributions import norm
from sklearn.neighbors import KernelDensity
import os



from subprocess import check_output
#print(check_output(["ls", "/home/saeed/Downloads"]).decode("utf8"))

data = pd.read_csv('/home/saeed/Downloads/emails_polished.csv')
LENGTH = len(data)




pd.options.mode.chained_assignment = None

chunk = pd.read_csv('/home/saeed/Downloads/emails.csv' , chunksize  = 9000)
data = next(chunk)


print(data.columns , end = '\n ######### \n')
print(data.message[1] , end = '\n ######### \n')

start_time = time.time()
def remove_noreceivers(df , Series):
    index_of_recepients = []
    indices = []
    patterncheck = re.compile('(From: )[\w\.-]+@[\w\.-]+\.[\w]+\s(To: )')

    for i in range(len(Series)):
        if bool(patterncheck.search(Series.values[i][:400])):
            index_of_recepients.append(i)
            print('This is row ' , i)

    # LEN_OF_INDEX = len(index_of_recepients)
    FileSeries = []
    MessageSeries = []
    CHUNKSIZE = 1000
    for i in range(len(index_of_recepients) // CHUNKSIZE):
        if i < (len(index_of_recepients) // CHUNKSIZE) - 1:
            CHUNKSIZE = 1000
            indices = []
            for j in range(CHUNKSIZE):
                indices.append(j)
                print('This is creating the new index ' , j , 'in stage ' , i)

            indices = pd.Index(indices)
            fileseries = pd.Series(index=indices)
            messageseries = pd.Series(index=indices)

            for j in range(CHUNKSIZE):
                fileseries.iloc[j] = df.file[index_of_recepients[j + i*CHUNKSIZE]]
                messageseries.iloc[j] = df.message[index_of_recepients[j + i*CHUNKSIZE]]
                print('This is writing shit in two new series ' , j , 'in stage ' , i)
            FileSeries.append(fileseries)
            MessageSeries.append(messageseries)
        else:
            LAST_CHUNKSIZE = CHUNKSIZE
            CHUNKSIZE = len(index_of_recepients) - ((len(index_of_recepients) // CHUNKSIZE) - 1)*CHUNKSIZE
            indices = []
            for j in range(CHUNKSIZE):
                indices.append(j)
                print('This is creating the new index ' , j , 'in stage ' , i)

            indices = pd.Index(indices)
            fileseries = pd.Series(index=indices)
            messageseries = pd.Series(index=indices)

            for j in range(CHUNKSIZE):
                fileseries.iloc[j] = df.file[index_of_recepients[j + i*LAST_CHUNKSIZE]]
                messageseries.iloc[j] = df.message[index_of_recepients[j + i*LAST_CHUNKSIZE]]
                print('This is writing shit in two new series ' , j , 'in stage ' , i)
            FileSeries.append(fileseries)
            MessageSeries.append(messageseries)

    FileSeries = pd.concat(FileSeries , ignore_index=True)
    MessageSeries = pd.concat(MessageSeries , ignore_index=True)

    frame = {'file' : FileSeries , 'message' : MessageSeries}
    dataFrame = pd.DataFrame(frame)

    NUMBER_OF_NORECEIVERS_DELETED = len(Series) - len(dataFrame)

    return (dataFrame , NUMBER_OF_NORECEIVERS_DELETED)





def get_receivers(df , Series):
    recepients = []
    index_of_recepients = []
    RECEPIENTS = []
    pattern = re.compile(r'\s(To: )([\w\.-]+@[\w\.-]+\.[\w]+,?[\s]+)+(?=Subject)')

    for i in range(len(Series)):
        matches = pattern.finditer(Series.values[i][:400])
        for match in matches:
            index_of_recepients.append(i)
            recepients.append(match.group())

    FileSeries = []
    MessageSeries = []
    CHUNKSIZE = 1000
    LEN_OF_INDEX = len(index_of_recepients)
    for i in range(LEN_OF_INDEX // CHUNKSIZE):
        if i < (LEN_OF_INDEX // CHUNKSIZE) - 1:
            CHUNKSIZE = 1000
            indices = []
            for j in range(CHUNKSIZE):
                indices.append(j)
                print('This is creating the new index ', j, 'in stage ', i)

            indices = pd.Index(indices)
            fileseries = pd.Series(index=indices)
            messageseries = pd.Series(index=indices)
            Recepients = pd.Series(index=indices)

            for j in range(CHUNKSIZE):
                fileseries.iloc[j] = df.file[index_of_recepients[j + i*CHUNKSIZE]]
                messageseries.iloc[j] = df.message[index_of_recepients[j + i*CHUNKSIZE]]
                Recepients.iloc[j] = recepients[j + i*CHUNKSIZE]
                print('This is writing shit in two new series ', j, 'in stage ', i)
            FileSeries.append(fileseries)
            MessageSeries.append(messageseries)
            RECEPIENTS.append(Recepients)
        else:
            LAST_CHUNKSIZE = CHUNKSIZE
            CHUNKSIZE = LEN_OF_INDEX - ((LEN_OF_INDEX // CHUNKSIZE) - 1) * CHUNKSIZE
            indices = []
            for j in range(CHUNKSIZE):
                indices.append(j)
                print('This is creating the new index ', j, 'in stage ', i)

            indices = pd.Index(indices)
            fileseries = pd.Series(index=indices)
            messageseries = pd.Series(index=indices)
            Recepients = pd.Series(index=indices)

            for j in range(CHUNKSIZE):
                fileseries.iloc[j] = df.file[index_of_recepients[j + i*LAST_CHUNKSIZE]]
                messageseries.iloc[j] = df.message[index_of_recepients[j + i*LAST_CHUNKSIZE]]
                Recepients.iloc[j] = recepients[j + i*LAST_CHUNKSIZE]
                print('This is writing shit in two new series ', j, 'in stage ', i)
            FileSeries.append(fileseries)
            MessageSeries.append(messageseries)
            RECEPIENTS.append(Recepients)

    FileSeries = pd.concat(FileSeries, ignore_index=True)
    MessageSeries = pd.concat(MessageSeries, ignore_index=True)
    RECEPIENTS = pd.concat(RECEPIENTS , ignore_index=True)

    frame = {'file': FileSeries, 'message': MessageSeries}
    dataFrame = pd.DataFrame(frame)

    NUMBER_OF_NONMATCHES_DELETED = len(Series) - len(dataFrame)

    return (RECEPIENTS , dataFrame , NUMBER_OF_NONMATCHES_DELETED)

################################################################################################################



def OneReceiverOnly(df , recepients):
    CHUNKSIZE = 1000
    LENGTH_OF_SERIES = len(df)
    Fileseries = []
    Messageseries = []
    Receiverseries = []
    Numberofreceivers = []
    pattern_of_Email = re.compile(r'[\w\.-]+@[\w\.-]+\.[\w]+')

    for i in range(LENGTH_OF_SERIES // CHUNKSIZE):
        if i < (LENGTH_OF_SERIES // CHUNKSIZE) - 1:
            fileseries1 = []
            messageseries1 = []
            receiverseries1 = []
            numberofreceivers1 = []
            for j in range(CHUNKSIZE):
                receivers = []
                matches = pattern_of_Email.finditer(recepients.values[j])
                for match in matches:
                    receivers.append(match.group())
                NUMBER_OF_RECEIVERS = len(receivers)
                print('Size of receiver is, in row, ' , j, 'equal to ' , NUMBER_OF_RECEIVERS)


                for m in range(NUMBER_OF_RECEIVERS):
                    fileseries1.append(df.file[j])
                    messageseries1.append(df.message[j])
                    receiverseries1.append(receivers[m])                ################## Check this
                    numberofreceivers1.append(NUMBER_OF_RECEIVERS)
            print('In chunk ' , i , '     the length of eventual list of file is        ' , len(fileseries1))
            print('In chunk ' , i , '     the length of eventual list of message is     ' , len(messageseries1))
            print('In chunk ' , i , '     the length of eventual list receiver is       ' , len(receiverseries1))
            print('In chunk ' , i , ' the length of eventual list number of receivers is' , len(numberofreceivers1))
            Fileseries.append(pd.Series(fileseries1))
            Messageseries.append(pd.Series(messageseries1))
            Receiverseries.append(pd.Series(receiverseries1))
            Numberofreceivers.append(pd.Series(numberofreceivers1))
        else:
            LAST_CHUNKSIZE = LENGTH_OF_SERIES - ((LENGTH_OF_SERIES // CHUNKSIZE) - 1) * CHUNKSIZE
            fileseries1 = []
            messageseries1 = []
            receiverseries1 = []
            numberofreceivers1 = []

            for j in range(i*CHUNKSIZE , i*CHUNKSIZE + LAST_CHUNKSIZE):
                receivers = []
                matches = pattern_of_Email.finditer(recepients.values[j])
                for match in matches:
                    receivers.append(match.group())
                NUMBER_OF_RECEIVERS = len(receivers)
                print('Size of receiver is, in row, ', j, 'equal to ', NUMBER_OF_RECEIVERS)

                for m in range(NUMBER_OF_RECEIVERS):
                    fileseries1.append(df.file[j])
                    messageseries1.append(df.message[j])
                    receiverseries1.append(receivers[m])  ################## Check this
                    numberofreceivers1.append(NUMBER_OF_RECEIVERS)
            print('In chunk ', i, '     the length of eventual list of file is        ', len(fileseries1))
            print('In chunk ', i, '     the length of eventual list of message is     ', len(messageseries1))
            print('In chunk ', i, '     the length of eventual list receiver is       ', len(receiverseries1))
            print('In chunk ', i, ' the length of eventual list number of receivers is', len(numberofreceivers1))
            Fileseries.append(pd.Series(fileseries1))
            Messageseries.append(pd.Series(messageseries1))
            Receiverseries.append(pd.Series(receiverseries1))
            Numberofreceivers.append(pd.Series(numberofreceivers1))

    Fileseries = pd.concat(Fileseries , ignore_index=True)
    Messageseries = pd.concat(Messageseries , ignore_index=True)
    Receiverseries = pd.concat(Receiverseries , ignore_index=True)
    Numberofreceivers = pd.concat(Numberofreceivers , ignore_index=True)

    frame = {'file': Fileseries, 'message': Messageseries, 'receivers': Receiverseries , 'Number_of_receivers': Numberofreceivers}
    dataOut = pd.DataFrame(frame)
    return dataOut



#################################################################################################################

def get_subject(Series, row_num = 3):
    """returns a single row split out from each message. Row_num is the index of the specific
    row that you want the function to return."""
    Result = []
    CHUNKSIZE = 1000
    LENGTH_OF_SERIES = len(Series)

    for i in range(LENGTH_OF_SERIES // CHUNKSIZE):
        if i < (LENGTH_OF_SERIES // CHUNKSIZE) - 1:
            CHUNKSIZE = 1000
            # indices = pd.Index([i for i in range(CHUNKSIZE)])
            # result = pd.Series(index=indices)
            result = []

            for j in range(i*CHUNKSIZE , (i+1)*CHUNKSIZE):
                subject_row = row_num
                message_words = Series[j].split('\n')
                while not message_words[subject_row].startswith('Subject:'):
                    subject_row = subject_row + 1
                message_words = message_words[subject_row]
                result.append(message_words)
            result = pd.Series(result)
            Result.append(result)
        else:
            CHUNKSIZEF = LENGTH_OF_SERIES - ((LENGTH_OF_SERIES // CHUNKSIZE) - 1) * CHUNKSIZE
            # indices = pd.Index([i for i in range(CHUNKSIZE)])
            # result = pd.Series(index=indices)
            result = []

            for j in range(i*CHUNKSIZE , i*CHUNKSIZE + CHUNKSIZEF):
                subject_row = row_num
                message_words = Series[j].split('\n')
                while not message_words[subject_row].startswith('Subject:'):
                    subject_row = subject_row + 1
                message_words = message_words[subject_row]
                result.append(message_words)
            result = pd.Series(result)
            Result.append(result)

    Result = pd.concat(Result , ignore_index=True)
    return Result





def get_row(Series, row_num):
    """returns a single row split out from each message. Row_num is the index of the specific
    row that you want the function to return."""
    Result = []
    # result = pd.Series(index=Series.index)
    CHUNKSIZE = 1000
    LENGTH_OF_SERIES = len(Series)

    for i in range(LENGTH_OF_SERIES // CHUNKSIZE):
        if i < (LENGTH_OF_SERIES // CHUNKSIZE) - 1:
            # CHUNKSIZE = 1000
            # indices = pd.Index([i for i in range(CHUNKSIZE)])
            # result = pd.Series(index=indices)
            result = []

            for j in range(i*CHUNKSIZE , (i+1)*CHUNKSIZE):
                message_words = Series[j].split('\n')
                message_words = message_words[row_num]
                result.append(message_words)
            result = pd.Series(result)
            Result.append(result)
        else:
            CHUNKSIZEF = LENGTH_OF_SERIES - ((LENGTH_OF_SERIES // CHUNKSIZE) - 1) * CHUNKSIZE
            # indices = pd.Index([i for i in range(CHUNKSIZE)])
            # result = pd.Series(index=indices)
            result = []

            for j in range(i*CHUNKSIZE , i*CHUNKSIZE + CHUNKSIZEF):
                message_words = Series[j].split('\n')
                message_words = message_words[row_num]
                result.append(message_words)
            result = pd.Series(result)
            Result.append(result)

    Result = pd.concat(Result, ignore_index=True)
    return Result



def get_datetime(Series):
    ROW_OF_DATE = 1
    # result = pd.Series(index = Series.index)
    Result = []
    CHUNKSIZE = 1000
    LENGTH_OF_SERIES = len(Series)
    for i in range(LENGTH_OF_SERIES // CHUNKSIZE):
        if i < (LENGTH_OF_SERIES // CHUNKSIZE) - 1:
            CHUNKSIZE = 1000
            # indices = pd.Index([i for i in range(CHUNKSIZE)])
            # result = pd.Series(index=indices)
            result = []

            for j in range(i*CHUNKSIZE , (i+1)*CHUNKSIZE):
                print('Now j is equal to ', j)
                message_words = Series.iloc[j].split('\n')
                date = message_words[ROW_OF_DATE].replace('Date: ', '')
                datetime_object = pd.to_datetime(date , infer_datetime_format=True)
                result.append(datetime_object)
            result = pd.Series(result)
            Result.append(result)
        else:
            print('Chunk size now is equal to ' , CHUNKSIZE)
            CHUNKSIZEF = LENGTH_OF_SERIES - ((LENGTH_OF_SERIES // CHUNKSIZE) - 1) * CHUNKSIZE
            print('Chunk size now is equal to ', CHUNKSIZE)
            # indices = pd.Index([i for i in range(CHUNKSIZE)])
            # result = pd.Series(index=indices)
            result = []
            print('i now is ', i , ' and j now is ', j)
            for j in range(i*CHUNKSIZE , i*CHUNKSIZE + CHUNKSIZEF):
                print('We are doing step ' , j , ' in this critical round')
                message_words = Series.iloc[j].split('\n')
                date = message_words[ROW_OF_DATE].replace('Date: ', '')
                datetime_object = pd.to_datetime(date, infer_datetime_format=True)
                result.append(datetime_object)
            result = pd.Series(result)
            Result.append(result)

    Result = pd.concat(Result , ignore_index=True)

    return Result






def get_senders(Series, row_num = 2):
    """returns a single row split out from each message. Row_num is the index of the specific
    row that you want the function to return."""
    Result = []
    result = pd.Series(index=Series.index)
    CHUNKSIZE = 1000
    LENGTH_OF_SERIES = len(Series)

    for i in range(LENGTH_OF_SERIES // CHUNKSIZE):
        if i < (LENGTH_OF_SERIES // CHUNKSIZE) - 1:
            CHUNKSIZE = 1000
            # indices = pd.Index([i for i in range(CHUNKSIZE)])
            # result = pd.Series(index=indices)
            result = []

            for j in range(i*CHUNKSIZE , (i+1)*CHUNKSIZE):
                message_words = Series[j].split('\n')
                message_words = message_words[row_num]
                result.append(message_words)
            result = pd.Series(result)
            Result.append(result)
        else:
            CHUNKSIZEF = LENGTH_OF_SERIES - ((LENGTH_OF_SERIES // CHUNKSIZE) - 1) * CHUNKSIZE
            # indices = pd.Index([i for i in range(CHUNKSIZE)])
            # result = pd.Series(index=indices)
            result = []

            for j in range(i*CHUNKSIZE , i*CHUNKSIZE + CHUNKSIZEF):
                message_words = Series[j].split('\n')
                message_words = message_words[row_num]
                result.append(message_words)
            result = pd.Series(result)
            Result.append(result)

    Result = pd.concat(Result, ignore_index=True)
    return Result





data, Num_OF_NORECEIVERS_DELETED = remove_noreceivers(data , data.message)

recepients, data, NUMBER_OF_NONMATCHES_DELETED = get_receivers(data , data.message)

data = OneReceiverOnly(data , recepients)



# took less than 8 minutes so far

for i in range(85):
    print('Number of receivers in row ' , i , ' is ' , data.Number_of_receivers[i] , ' and the receiver is ' , data.receivers[i])

#A series containing date and time of Emails
# Took 500 seconds

date_time = get_datetime(data.message)



print(type(date_time))        #Series
print(type(date_time.values)) #array

# We are not using Email text as of now
# Email_text = get_text(data.message , 15)

message_ID = get_row(data.message , 0)   # a Series that contains message IDs   Took 5.6 seconds



Senders_of_Emails =  get_senders(data.message , 2)    # Took 5.43 seconds




# pattern_of_Email = re.compile(r'[\w\.-]+@[\w\.-]+\.[\w]+')
for i in range(len(Senders_of_Emails)):
    # matches = pattern_of_Email.finditer(Senders_of_Emails[i])
    print('Now doing row ', i)
    # for match in matches:
    #     sender = match.group()
    # Senders_of_Emails.iloc[i] = sender
    Senders_of_Emails[i] = Senders_of_Emails[i].replace('From: ' , '')



Subjects_of_Emails = get_subject(data.message , 3)

Number_of_Receivers = data.Number_of_receivers

recepients = data.receivers



frame = {'file': data.file , 'message': data.message ,'Message_ID': message_ID, 'date': date_time , 'From': Senders_of_Emails , 'Subject': Subjects_of_Emails , 'receivers': recepients , 'Number_of_receivers': Number_of_Receivers}
data = pd.DataFrame(frame)

data.to_csv(r'/home/saeed/Downloads/emails_polished.csv' , index = False)
print(time.time() - start_time)

# Finding recepients of Allen in Enron and their corresponding Frequency :::: This portion takes time 40 minutes

LENGTH = len(data)
Allen_receivers = []
for i in range(LENGTH):
    print('We are now in row ' , i)
    if data.From[i] == 'phillip.allen@enron.com':
        Allen_receivers.append(data.receivers[i])

Allen_receivers_Unique = np.unique(Allen_receivers)

start_time = time.time()
LENGTH = len(data)
ALLEN_RECEIVERS_UNIQUE = len(Allen_receivers_Unique)
Allen_receivers_UniqueFreq = []
for i in range(ALLEN_RECEIVERS_UNIQUE):
    print('We are doing receiver ', i)
    count = 0

    for j in range(LENGTH):
        if data.From[j] == 'phillip.allen@enron.com' and data.receivers[j] == Allen_receivers_Unique[i]:
            count += 1
    Allen_receivers_UniqueFreq.append([Allen_receivers_Unique[i], count])
print(time.time() - start_time)


a = np.array([Allen_receivers_UniqueFreq[i][1] for i in range(ALLEN_RECEIVERS_UNIQUE)])
b = Allen_receivers_UniqueFreq[Allen_receivers_UniqueFreq[i][1] == 29784]

# Note that keith.holst@enron.com received the second most emails from Allen

LENGTH = len(data)
Message_ID = []
date = []
From = []
To = []
Subject = []
Number_of_receivers = []
for i in range(LENGTH):
    print('In row ' , i)
    if data.From[i] == 'phillip.allen@enron.com' and data.receivers[i] == 'keith.holst@enron.com':
        Message_ID.append(data.Message_ID[i])
        date.append(data.date[i])
        From.append(data.From[i])
        To.append(data.receivers[i])
        Subject.append(data.Subject[i])
        Number_of_receivers.append(data.Number_of_receivers[i])

Message_ID = pd.Series(Message_ID)
date = pd.Series(date)
From = pd.Series(From)
To = pd.Series(To)
Subject = pd.Series(Subject)
Number_of_receivers = pd.Series(Number_of_receivers)

frame = {'Message_ID': Message_ID , 'date': date , 'From': From , 'To': To , 'Subject': Subject , 'Number_of_receivers': Number_of_receivers}
AllenToKeith = pd.DataFrame(frame)

# Now emails Keith received from Allen

Message_ID = []
date = []
From = []
To = []
Subject = []
Number_of_receivers = []
for i in range(LENGTH):
    print('In row ' , i)
    if data.From[i] == 'keith.holst@enron.com' and data.receivers[i] == 'phillip.allen@enron.com':
        Message_ID.append(data.Message_ID[i])
        date.append(data.date[i])
        From.append(data.From[i])
        To.append(data.receivers[i])
        Subject.append(data.Subject[i])
        Number_of_receivers.append(data.Number_of_receivers[i])

Message_ID = pd.Series(Message_ID)
date = pd.Series(date)
From = pd.Series(From)
To = pd.Series(To)
Subject = pd.Series(Subject)
Number_of_receivers = pd.Series(Number_of_receivers)

frame = {'Message_ID': Message_ID , 'date': date , 'From': From , 'To': To , 'Subject': Subject , 'Number_of_receivers': Number_of_receivers}
KeithToAllen = pd.DataFrame(frame)

############# Keith to anyone

sent = []
for i in range(LENGTH):
    if 'From: keith.holst@enron.com' in data.message[i]:
        sent.append(data.message[i])

#############
# Finding emails Allen received from people

Allen_received = []
for i in range(LENGTH):
    print('We are now in row ' , i)
    if data.receivers[i] == 'phillip.allen@enron.com':
        Allen_received.append(data.From[i])

Allen_received_Unique = np.unique(Allen_received)

start_time = time.time()
ALLEN_RECEIVED_UNIQUE = len(Allen_received_Unique)
Allen_received_UniqueFreq = []
for i in range(ALLEN_RECEIVED_UNIQUE):
    print('We are doing receiver ', i)
    count = 0

    for j in range(LENGTH):
        if data.receivers[j] == 'phillip.allen@enron.com' and data.From[j] == Allen_received_Unique[i]:
            count += 1
    Allen_received_UniqueFreq.append([Allen_received_Unique[i], count])
print(time.time() - start_time)


a = np.array([Allen_receivers_UniqueFreq[i][1] for i in range(ALLEN_RECEIVERS_UNIQUE)])
b = Allen_receivers_UniqueFreq[Allen_receivers_UniqueFreq[i][1] == 29784]


########################################## More General

# finding the list of Unique Emails who sent at least one Email

EmailList = []
for i in range(LENGTH):
    if not data.From[i] in EmailList:
        EmailList.append(data.From[i])

# Now finding those ***@enron.com Emails among EmailList

patternEnronEmail = re.compile(r'[\w\.-]+@enron\.com')
EnronEmails = []
for i in range(len(EmailList)):
    matches = patternEnronEmail.finditer(EmailList[i])
    for match in matches:
        EnronEmails.append(match.group())


EmailsSentReceived = []

for i in range(len(EnronEmails)):
    received = len(data.receivers[data.receivers == EnronEmails[i]])
    sent = len(data.receivers[(data.From == EnronEmails[i]) & (data.Number_of_receivers == 1)])
    EmailsSentReceived.append([EnronEmails[i] , sent , received])

# Now finding how many emails each email address has sent and received
# Notice this takes about 40 minutes

EmailsSentReceived = []
for i in range(ENRONLENGTH):
    print('We are now taking care of Email number ', i)
    SentEmails = 0
    ReceivedEmails = 0
    for j in range(LENGTH):
        if data.From[j] == EnronEmails[i] and data.receivers[j] != EnronEmails[i]:
            SentEmails += 1
        elif data.receivers[j] == EnronEmails[i] and data.From[j] != EnronEmails[i]:
            ReceivedEmails += 1
        elif data.From[j] == EnronEmails[i] and data.receivers[j] == EnronEmails[i]:
            SentEmails += 1
            ReceivedEmails += 1
    EmailsSentReceived.append([EnronEmails[i] , SentEmails , ReceivedEmails])


########################################################################################################################


data = pd.read_csv('/home/saeed/Downloads/emails_polished.csv')

data = data.drop(columns = 'Unnamed: 0')
data = data.drop(columns = 'file')
data = data.drop(columns = 'Subject')
data = data.drop(columns = 'message')

data = data.sort_values(by = 'date')
data = data.reset_index(drop = True)

data = data[513200:514000]

data = data.reset_index(drop = True)

LENGTH = len(data)

Bob = []
Andy = []
for i in range(LENGTH):
    if data.From[i] == 'bob.shults@enron.com':
        Bob.append(1)
        Andy.append(-1)
    elif data.From[i] == 'andy.zipper@enron.com':
        Bob.append(-1)
        Andy.append(1)
    else:
        Bob.append(-1)
        Andy.append(-1)
Bob = pd.Series(np.array(Bob))
Andy = pd.Series(np.array(Andy))

data['Bob'] = Bob
data['Andy'] = Andy



data.set_index('date' , inplace = True)   # Set dates as index of dataframe BobAndy

# data['Bob'].plot(linewidth=0.5)
# plt.show()
# data['Andy'].plot(linewidth=0.5)
# plt.show()

people = ['Bob' , 'Andy']
axes = data[people].plot(alpha=0.5, figsize=(11, 9), subplots=True)
for i in range(len(axes)):
    axes[i].set_ylabel(people[i])
plt.show()
plt.savefig('/home/saeed/Downloads/Documents/plot.png')

# DateArray = np.linspace(pd.Timestamp("10-MAY-2001 23:09:19.445506").value, pd.Timestamp("8-OCT-2001 01:09:22.404973").value, 100000, dtype=np.int64)
# DateArray = pd.DatetimeIndex(DateArray)




people = ['Bob' , 'Andy']
axes = data[['Bob' , 'Andy']].plot(marker='.', alpha=0.5, linestyle='None', figsize=(11, 9), subplots=True)
for i in range(len(axes)):
    axes[i].set_ylabel(people[i])
plt.show()

######## Creating equispaced time axis
dates_of_emails = pd.to_datetime(data.date , infer_datetime_format=True)
dates_of_emails = dates_of_emails.values.tolist()
dates_of_emails.sort()
# for i in range(15000, 90000):
#     print(dates_of_emails[i])
#     time.sleep(0.0005)

start_date = dates_of_emails[4818]
end_date = dates_of_emails[514877]

# Changing to TimeStamp to make it linespace-able
start_date = pd.Timestamp(start_date)
end_date = pd.Timestamp(end_date)
times = np.linspace(start_date.value , end_date.value , 500000)
times = pd.to_datetime(times)

data = data.sort_values(by = 'date')
data = data.reset_index(drop = True)

# This takes about 30 minutes to run

start_time = time.time()
Phillip = np.zeros(len(times))
Phillip = Phillip - 1
count = 0
PhillipDates = []
for j in EnronEmails:

    for i in range(LENGTH):
        if data.From[i] == 'john.griffith@enron.com':
            print('We are in row ' , i)
            # if data.From[i] == 'ina.rangel@enron.com':
            count += 1
            x = pd.to_datetime(data.date[i])
            x = x.replace(tzinfo=None)
            idx = (np.abs(times - x)).argmin()
            if not x in PhillipDates:
                PhillipDates.append(x)
                Phillip[idx] += 2
print(start_time - time.time())

plt.plot(times[300000:315000],Phillip[300000:315000],c='red', ls='', ms=5, marker='x')
ax = plt.gca()
plt.xlabel('Dates of Communication')
plt.ylabel('Phillip Allen Communications')
plt.title('Part of Phillip Allen\'s communication')
plt.show()

plt.savefig('/home/saeed/Desktop/PhillipAllen.png')
plt.show()

plt.hist(Ina, 500000, density=0, facecolor='g', alpha=0.75)
plt.show()

## For Andy Zipper

start_time = time.time()
Andy = np.zeros(len(times))
Andy = Andy - 1

AndyDates = []

for i in range(LENGTH):
    if data.From[i] == 'andy.zipper@enron.com':
        # if data.From[i] == 'ina.rangel@enron.com':
        x = pd.to_datetime(data.date[i])
        x = x.replace(tzinfo=None)
        idx = (np.abs(times - x)).argmin()
        if not x in AndyDates:
            AndyDates.append(x)
            Andy[idx] += 2
Andy[Andy>20] = 3

print(time.time() - start_time)

# or

Andy = data[data.From == 'andy.zipper@enron.com']
Andy = Andy.sort_values(by = 'date')
Andy = Andy.reset_index(drop = True)
Andydates = pd.to_datetime(Andy.date , infer_datetime_format = True)
Andydates = Andydates.values.tolist()

# Build up on this one
frame = {'date': data.date , 'SendDate': data.date[data.From == 'andy.zipper@enron.com']}
Andy = pd.DataFrame(frame)
indices = np.where(Andy.date == Andy.SendDate)[0]
Andy.SendDate[indices] = 1
Andy.SendDate[indices] = 1
Andy.SendDate[Andy.SendDate != 1] = 0
Andy.set_index('date' , inplace = True)   # Set dates as index of dataframe BobAndy

#
fig2 = plt.figure(figsize=(14.0, 9.0))
plt.plot(times,Andy,c='red', ls='', ms=5, marker='|')
ax = plt.gca()
plt.xlabel('Dates of Communication')
plt.ylabel('Andy Zipper Communications')
plt.title('Part of Andy Zipper\'s communication')
plt.savefig('/home/saeed/Desktop/test.png')
plt.close()
plt.cla()
plt.clf()


fig2 = plt.figure(figsize=(14.0, 9.0))
plt.plot(times,Andy,c='red', ls='', ms=5, marker='|')
ax = plt.gca()
plt.xlabel('Dates of Communication')
plt.ylabel('Andy Zipper Communications')
plt.title('Part of Andy Zipper\'s communication')
plt.savefig('/home/saeed/Desktop/test.png')
plt.show()

indices = np.where(Andy > 0)[0]
Email_times = times[indices]  # datetime.DatetimeIndex type
Waiting_Time = []
WAIT = len(Email_times) - 1
for i in range(WAIT):
    Waiting_Time.append(Email_times[i+1] - Email_times[i])
waiting_min = np.min(Waiting_Time)
waiting_max = np.max(Waiting_Time)

waiting_min_seconds = int(np.floor(waiting_min.total_seconds()))
waiting_max_seconds = int(np.ceil(waiting_max.total_seconds()))

PIECES = waiting_max_seconds - waiting_min_seconds

PIECES = PIECES//100

waiting_time_axis = np.linspace(waiting_min.total_seconds() , waiting_max.total_seconds() , PIECES)
waitingHist = np.zeros(len(waiting_time_axis))

for i in range(WAIT):
    idx = np.abs(waiting_time_axis - Waiting_Time[i].total_seconds()).argmin()
    waitingHist[idx] += 1

fig2 = plt.figure(figsize=(14.0, 9.0))
plt.plot(waiting_time_axis[:1000], waitingHist[:1000], c='red', ls='solid', ms=5, marker='*')
ax = plt.gca()
plt.savefig('/home/saeed/Desktop/testWaiting.png')

########################################################################################################################
# Save Everyone

start_time = time.time()

for j in EnronEmails[1:]:
    print('We are plotting Email ', j)
    Person = np.zeros(len(times))
    Person = Person - 1
    count = 0
    PersonDates = []
    for i in range(LENGTH):
        if data.From[i] == j:

            # if data.From[i] == 'ina.rangel@enron.com':
            count += 1
            x = pd.to_datetime(data.date[i])
            x = x.replace(tzinfo=None)
            idx = (np.abs(times - x)).argmin()
            if not x in PersonDates:
                PersonDates.append(x)
                Person[idx] += 2
    Person[Person > 20] = 3
    fig2 = plt.figure(figsize=(14.0, 9.0))
    plt.plot(times, Person, c='red', ls='', ms=5, marker='|')
    ax = plt.gca()
    plt.xlabel('Dates of Communication')
    plt.ylabel('Communications of '+ j.split('@')[0])
    plt.title('Part of '+ j.split('@')[0]+ '\'s communication')
    plt.savefig('/home/saeed/Desktop/Emails/'+j.split('@')[0]+'.png')
    plt.close()
    plt.cla()
    plt.clf()
print(time.time() - start_time)

plt.show()


plt.show()





################################################
# Waiting Time for other Emails

for j in EnronEmails[1:]:
    print('We are plotting Email ', j)
    Person = np.zeros(len(times))
    Person = Person - 1
    count = 0
    PersonDates = []
    for i in range(LENGTH):
        if data.From[i] == j:

            # if data.From[i] == 'ina.rangel@enron.com':
            count += 1
            x = pd.to_datetime(data.date[i])
            x = x.replace(tzinfo=None)
            idx = (np.abs(times - x)).argmin()
            if not x in PersonDates:
                PersonDates.append(x)
                Person[idx] += 2
    Person[Person > 20] = 3


    indices = np.where(Person > 0)[0]
    Email_times = times[indices]  # datetime.DatetimeIndex type
    Waiting_Time = []
    WAIT = len(Email_times) - 1
    if WAIT > 1:
        for i in range(WAIT):
            Waiting_Time.append(Email_times[i + 1] - Email_times[i])
        waiting_min = np.min(Waiting_Time)
        waiting_max = np.max(Waiting_Time)

        waiting_min_seconds = int(np.floor(waiting_min.total_seconds()))
        waiting_max_seconds = int(np.ceil(waiting_max.total_seconds()))

        PIECES = waiting_max_seconds - waiting_min_seconds

        PIECES = PIECES // 100

        waiting_time_axis = np.linspace(waiting_min.total_seconds(), waiting_max.total_seconds(), PIECES)
        waitingHist = np.zeros(len(waiting_time_axis))

        for i in range(WAIT):
            idx = np.abs(waiting_time_axis - Waiting_Time[i].total_seconds()).argmin()
            waitingHist[idx] += 1

        fig2 = plt.figure(figsize=(14.0, 9.0))
        plt.plot(waiting_time_axis, waitingHist, c='red', ls='solid', ms=5, marker='*')
        ax = plt.gca()
        plt.xlabel('Waiting Times in seconds')
        plt.ylabel('Voting of waiting times for '+ j.split('@')[0])
        plt.title('Empirical waiting time distribution for '+ j.split('@')[0]+ '\'s communication')
        plt.savefig('/home/saeed/Desktop/Emails/waiting times/'+j.split('@')[0]+'.png')
        plt.close()
        plt.cla()
        plt.clf()


##########################
# Test for bad Emails

Person = np.zeros(len(times))
Person = Person - 1
count = 0
PersonDates = []
for i in range(LENGTH):
    if data.From[i] == 'laura.levy@enron.com':
        count += 1
        x = pd.to_datetime(data.date[i])
        x = x.replace(tzinfo=None)
        idx = (np.abs(times - x)).argmin()
        if not x in PersonDates:
            PersonDates.append(x)
            Person[idx] += 2
Person[Person > 20] = 3


##########################


#################################################


data = pd.read_csv('/home/saeed/Downloads/emails_polished.csv')

data = data.drop(columns = 'Unnamed: 0')
data = data.drop(columns = 'file')
data = data.drop(columns = 'Subject')
data = data.drop(columns = 'message')

data = data.sort_values(by = 'date')
data = data.reset_index(drop = True)

data = data[288000:300000]

data = data.reset_index(drop = True)

LENGTH = len(data)

FHayden = []
PAllen = []
for i in range(LENGTH):
    if data.From[i] == 'christi.nicolay@enron.com':
        FHayden.append(1)
        PAllen.append(-1)
    elif data.From[i] == 'phillip.allen@enron.com':
        FHayden.append(-1)
        PAllen.append(1)
    else:
        FHayden.append(-1)
        PAllen.append(-1)
Bob = pd.Series(np.array(FHayden))
Andy = pd.Series(np.array(PAllen))

data['FrankHayden'] = FHayden
data['PhillipAllen'] = PAllen



data.set_index('date' , inplace = True)   # Set dates as index of dataframe BobAndy

data['FrankHayden'].plot(linewidth=0.5)
plt.show()
# data['Andy'].plot(linewidth=0.5)
# plt.show()

people = ['FrankHayden' , 'PhillipAllen']
axes = data[people].plot(alpha=0.5, figsize=(11, 9), subplots=True)
for i in range(len(axes)):
    axes[i].set_ylabel(people[i])
plt.show()


people = ['FrankHayden' , 'PhillipAllen']
axes = data[people].plot(marker='.', alpha=0.5, linestyle='None', figsize=(11, 9), subplots=True)
for i in range(len(axes)):
    axes[i].set_ylabel(people[i])
plt.show()


#########################################################################################
print(type(data_polished))
print(data_polished.columns)
print(data_polished['date'])
print('This is subject before being sort')
print(data_polished['Subject'])

before = data_polished['Subject'].values


print('Now sorting data by datetime')
data_polished_sorted = data_polished.sort_values(by = 'date')


data_polished_sorted = data_polished_sorted.reset_index()

data_polished_sorted = data_polished_sorted.drop(columns='index')


after = data_polished_sorted['Subject'].values



# Test to check if data sorted by date is different than initial data (different mostly)
count = 0
for i in range(len(data_polished_sorted)):
    if before[i] == after[i]:
        count += 1

print(count)



print()

print(type(Email_text))  #Should be Series?????
print(type(Email_text.values)) #Should be array???
print(type(Email_text.values[5]))  #Should be list??
print(len(Email_text.values))


# Should give you different lengths as each Email has different length (i.e. different
# number of new line characters)

# This section searches all the Emails in the portion selected and find the one
# with the maximum number of new line characters (maybe that one has biggest text
# body as well
list_of_textLength = []
for i in range(len(Email_text)):
    list_of_textLength.append(len(Email_text.values[i]))

index_of_Email, value_of_length = max(enumerate(list_of_textLength), key=operator.itemgetter(1))
print(index_of_Email , value_of_length)


# Returns a Series with elements equal to the important 13 digit identifiers
# This function gets the data_polished_sorted['Message ID']
def extract_message_ID(Series):
    result = pd.Series(index=Series.index)
    pattern = re.compile(r'\d{13}')
    for i in range(len(Series)):
        matches = pattern.finditer(Series.values[i])
        for match in matches:
            result.iloc[i] = match.group()
    return result

# for i in range(len(data_polished_sorted)):
#     print(data_polished_sorted['Message ID'].values[i])

# All the message ID's start with string Message-ID
Message_IDs = extract_message_ID(data_polished_sorted['Message ID'])
for i in range(len(Message_IDs)):
    print(Message_IDs.values[i])



for i in range(len(data_polished_sorted)):
    print(data_polished_sorted['date'].values[i])


for i in range(10):
    print(data_polished_sorted['Subject'].values[i])


for i in range(len(data_polished_sorted)):
    print(data_polished_sorted['Message ID'].values[i])

print(type(Message_IDs)) # is a Series
data_polished_sorted['13 digit ID'] = Message_IDs.values


# They are the same
for i in range(15):
    print(data_polished_sorted['Message ID'].values[i] , '                   ' , data_polished_sorted['13 digit ID'].values[i])




data_polished_sorted1 = data_polished_sorted.sort_values(by = '13 digit ID')


data_polished_sorted1 = data_polished_sorted1.reset_index()

data_polished_sorted1 = data_polished_sorted1.drop(columns='index')



for i in range(60 , 1000):
    print(data_polished_sorted1['13 digit ID'].values[i])

for i in range(60 , 1000):
    print(data_polished_sorted1['From'].values[i])

for i in range(60 , 1000):
    print(data_polished_sorted1['Subject'].values[i])


print(data_polished_sorted1.columns)


############################################ Reading the roles_files

roles_file = open(r'/home/saeed/Desktop/roles.txt' , 'r+')
roles = roles_file.read()

roles = roles.split('\n')
roles.remove(roles[-1])
roles.remove(roles[139])  # This was an anomaly in the text file and used
                          # as a broadcast proxy
idx = []

for i in range(len(roles)):
    print('We are in row ', i)
    if 'xxx' in roles[i]:
        idx.append(i)


for i in reversed(idx):
    roles.remove(roles[i])
    print('row ', i, ' removed and length of roles now is ', len(roles))

NetID = []
rest = []
for i in range(len(roles)):
    NetID.append(roles[i].split('\t')[0])
    rest.append(roles[i].split('\t')[1])

pattern = re.compile(r'[\w]+(\s)[\w]+')

names = []
notstrings = ['Vice President' , 'Risk Management' , 'Managing Director' ,
              'Regulatory Affairs' , 'Management Officer' , 'Financial Officer' ,
              'and Treasurer' , 'Also Chief' , 'Administrative Asisstant' ,
              'Senior Specialist' , 'Pipeline Business' , 'Real time' ,
              'Enery marketting', 'and trading', 'Chief of', 'Enron North',
              'America and', 'Enron Enery', 'Enron Gas', 'Chief Operating',
              'Chief Risk', 'Enron WholeSale', 'Enron Online', 'Trading Desk',
              'Logistics Manager', 'Enron Global', 'Government Affairs',
              'Legal Department', 'America and', 'Enron Enery', 'Senior Analyst',
              'In House', 'Government Relation', 'Enron America', 'Analyst Risk',
              'Cash Analyst']

for i in range(len(rest)):
    matches = pattern.finditer(rest[i])
    for match in matches:
        a = match.group()
        if a not in notstrings:
            names.append(a)
len(names)

roles = []
for i in range(len(rest)):
    pattern = re.compile(names[i] + r'[\s]+')
    matches = pattern.finditer(rest[i])

    for match in matches:
        roles.append(rest[i].split(match.group())[1])

EnronNetID = []
for i in range(len(EnronEmails)):
    EnronNetID.append(EnronEmails[i].split('@')[0])

InDataset = []

for i in range(len(NetID)):
    for j in range(len(EnronNetID)):
        added = 0
        if NetID[i] == EnronNetID[j]:
            InDataset.append(1)
            added += 1
            break
    if added == 0:
        InDataset.append(0)


frame = {'NetID': NetID , 'Names': names , 'Roles': roles , 'InDataset': InDataset}

Employees = pd.DataFrame(frame)





############## OS.WALK
# os.chdir("/home/saeed/Desktop/enron_mail_20150507/maildir")
# for root, dirs, files in os.walk("."):
#     print(root)



root_dir = "/home/saeed/Desktop/enron_mail_20150507/maildir/allen-p/straw"

for root, dir, files in os.walk(root_dir):
    for i in files:
        print(root+'/'+i)

roles_file = open(r'/home/saeed/Desktop/roles.txt' , 'r+')
roles = roles_file.read()

col1 = []
col2 = []

count = 0
# root_dir = "/home/saeed/Desktop/enron_mail_20150507/maildir"
os.chdir(r"/home/saeed/Desktop/enron_mail_20150507/maildir")
for root, dir, files in os.walk("."):
    for i in files:
        try:
            roles_file = open(root + '/' + i, 'r+')
            roles = roles_file.read()
        except UnicodeDecodeError:
            roles_file = open(root + '/' + i, 'r+' , encoding='cp1252')
            roles = roles_file.read()
        col1.append(root)
        col2.append(roles)
        count += 1
        print('We are doing email number ', count)

fileseries = np.asarray(col1)
fileseries = pd.Series(fileseries)

CHUNKSIZE = 1000
messageseries = []
for i in range(517000 // CHUNKSIZE):
    print('We are in row ' , i)
    test = np.asarray(col2[i*CHUNKSIZE:(i+1)*CHUNKSIZE])
    test = pd.Series(test)
    messageseries.append(test)

