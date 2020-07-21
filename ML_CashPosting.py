import pandas as pd
import pyodbc
import datetime
import re
import logging
import os.path
from datetime import datetime
from ftfy import fix_text
from fuzzywuzzy import fuzz
from sklearn.neighbors import NearestNeighbors
from sklearn.feature_extraction.text import TfidfVectorizer
import csv
import shutil
import time


def initialize_logger(output_dir, log_name):
    """
    :param output_dir: log file directory
    :param log_name: log file name
    """
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    # create console handler and set level to info
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s", "%Y-%m-%d %H:%M:%S")
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # create error file handler and set level to error
    handler = logging.FileHandler(os.path.join(output_dir, log_name + "_Error.log"), "w", encoding=None, delay="true")
    handler.setLevel(logging.ERROR)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s", "%Y-%m-%d %H:%M:%S")
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # create debug file handler and set level to debug
    handler = logging.FileHandler(os.path.join(output_dir, log_name + "_All.log"), "w")
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s", "%Y-%m-%d %H:%M:%S")
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def exec_sql(server, database, sql_procedure, params):
    """
    :param server: The name of the server name (JZNVCS)
    :param database: The name of the database (VCS)
    :param sql_procedure: The name of the sql procedure
    :param params: The parameters of the sql procedure
    :return: Nothing, otherwise sql error
    """
    conn = pyodbc.connect("Driver={SQL Server};"
                          "Server=" + server + ";"
                          "Database=" + database + ";"
                          "Trusted_Connection=yes;")
    conn.autocommit = True
    cursor = conn.cursor()
    sql_procedure = 'EXEC ' + sql_procedure

    try:
        if params == '':
            cursor.execute(sql_procedure)
        else:
            cursor.execute(sql_procedure, params)

        rows = cursor.fetchall()
        while rows:
            if cursor.nextset():
                rows = cursor.fetchall()
            else:
                rows = None
        cursor.commit()
        logging.info(database + ' - SQL procedure executed successfully!')
    except Exception as e:
        if str(e) == 'No results.  Previous SQL was not a query.':
            logging.info(database + ' - SQL procedure executed successfully! - No results')
            cursor.commit()
        else:
            logging.error(str(e))
    finally:
        conn.close


def create_excel(data, file_name):
    """
    :param data: Result data set
    :param file_name: Excel file name
    :return:
    """

    writer = pd.ExcelWriter(file_name, engine='xlsxwriter')
    data.to_excel(writer, sheet_name='Sheet1', index=False)

    # Get the xlsxwriter workbook and worksheet objects.
    workbook = writer.book
    worksheet = writer.sheets['Sheet1']

    # Add a header format.
    header_format = workbook.add_format({
        'bold': True,
        'text_wrap': True,
        'valign': 'vcenter',
        'align': 'center',
        'fg_color': '#D7E4BC',
        'border': 1})

    note_format = workbook.add_format({
        'text_wrap': True,
        'valign': 'top'})

    money_format = workbook.add_format({
        'text_wrap': True,
        'valign': 'top',
        'num_format': '#,##0.00'})

    header_name = [
        'Account #',
        'Account Note',
        'Matching Note',
        'Predicted Resolution',
        'Predicted Balance',
        'Match Confidence (Lower is Better)',
        'Similarity Ratio (Higher is Better)',
        'Result Category'
    ]

    worksheet.set_column('A:A', 25)
    worksheet.set_column('B:B', 50)
    worksheet.set_column('C:C', 50)
    worksheet.set_column('D:D', 25)
    worksheet.set_column('E:E', 12)
    worksheet.set_column('F:F', 14)
    worksheet.set_column('G:G', 14)
    worksheet.set_column('H:H', 25)

    # Write the column headers with the defined format.
    for col_num, value in enumerate(header_name):
        worksheet.write(0, col_num, value, header_format)

    for row_num, value in enumerate(data.AccountNo):
        worksheet.write(row_num + 1, 0, value, note_format)

    for row_num, value in enumerate(data.OriginalNote):
        worksheet.write(row_num + 1, 1, value, note_format)

    for row_num, value in enumerate(data.MatchedNote):
        worksheet.write(row_num + 1, 2, value, note_format)

    for row_num, value in enumerate(data.MatchedResolution):
        worksheet.write(row_num + 1, 3, value, note_format)

    for row_num, value in enumerate(data.NewBalance):
        worksheet.write(row_num + 1, 4, float(value), money_format)

    for row_num, value in enumerate(data.MatchConfidence):
        worksheet.write(row_num + 1, 5, value, note_format)

    for row_num, value in enumerate(data.SimilarityRatio):
        worksheet.write(row_num + 1, 6, value, note_format)

    for row_num, value in enumerate(data.ResultCategory):
        worksheet.write(row_num + 1, 7, value, note_format)

    # Close the Pandas Excel writer and output the Excel file.
    writer.save()


def check_keywords(sentence):
    """
    :param sentence: String to be checked
    :return: Number of keywords
    """
    keywords = ['ADJUSTMENT',
                'PAID',
                'FULL',
                'BALANCE',
                'CLOSE',
                'ADJ',
                'ADJUST',
                'PAYMENT',
                'BAL',
                'PYMT',
                'PMT',
                'RESP',
                'RESPONSIBILITY',
                'PAY',
                'PT',
                'PATIENT'
                'PIF']

    raw_sentence = set(str(sentence).split(' '))
    clean_sentence = " ".join(raw_sentence)
    matches = re.findall(r"(?=\b("+'|'.join(keywords)+r")\b)",clean_sentence)
    return len(matches)


def remove_hospital_notes(note):
    head, sep, tail = note.partition('PAT#')

    if head == '':
        return note
    else:
        return head


def clean_note(n):
    """
    :param n: Raw text to clean
    :return: Clean Text
    """
    n = n.replace('$', ' ')
    n = n.replace(',', '')
    n = " ".join(n.split())
    return n


def get_n_grams(string, n=3):
    """
    :param string: String to be checked
    :param n: Length of the grams
    :return: All grams
    """
    string = fix_text(string)
    string = string.encode("ascii", errors="ignore").decode()
    string = string.lower()
    chars_to_remove = [")", "(", ".", "|", "[", "]", "{", "}", "'"]
    rx = '[' + re.escape(''.join(chars_to_remove)) + ']'
    string = re.sub(rx, '', string)
    string = string.replace('&', 'and')
    string = string.replace(',', ' ')
    string = string.replace('-', ' ')
    string = string.title()
    string = re.sub(' +', ' ', string).strip()
    string = ' ' + string + ' '
    string = re.sub(r'[,-./]|\sBD', r'', string)
    n_grams = zip(*[string[i:] for i in range(n)])
    return [''.join(ngram) for ngram in n_grams]


def get_new_balance(matched_balance, matched_note, note):
    """
    :param matched_balance: Balance from the best match
    :param matched_note: Note form the best match
    :param note: Original note
    :return: The new balance
    """
    j_check = list()
    m_check = list()
    final_list = list()

    if matched_balance != 0.00:

        # J check
        keywords = re.findall(r"\w+(?=\s+" + str(matched_balance) + r")", matched_note)

        for word in keywords:
            matches = re.finditer(word + r" [+-]?([0-9]*[.])?[0-9]+", note)
            for m in matches:
                value = str(m.group()).replace(word, '').strip()
                if value not in j_check:
                    j_check.append(value)

        # M check
        patterns = [
            r'ADJ[A-Za-z]*\sBAL[A-Za-z]*\s[+-]?[0-9]*[.][0-9]+',
            r'PT[A-Za-z]*\sRESP[A-Za-z]*\s[IS|OF]+\s[+-]?[0-9]*[.][0-9]+',
            r'BILL[A-Za-z]*\sPT[A-Za-z]*\s[FOR]+\s[+-]?[0-9]*[.][0-9]+',
            r'[.0-9]* IS PT[A-Za-z]*\sRESP[A-Za-z]*' # pending review by M
        ]

        regex = "(" + ")|(".join(patterns) + ")"

        matches = re.findall(regex, note)

        for m in matches:
            value = re.sub('[^.0-9]+', '', str(m))
            if value not in m_check and value != '':
                m_check.append(value)

    combine_check = j_check + list(set(m_check) - set(j_check))

    if len(combine_check) == 1:
        return combine_check[0]

    if len(combine_check) == 0:
        return 0

    if len(combine_check) > 1:
        if len(j_check) < len(m_check):
            min_list = j_check
            max_list = m_check
        else:
            min_list = m_check
            max_list = j_check

        for element in min_list:
            if element in max_list:
                final_list.append(element)

        if len(final_list) == 1:
            return final_list[0]
        else:
            return 0


def get_nearest_neighbor(engine, algorithm, query):
    """
    :param engine: Engine object
    :param algorithm: algorithm
    :param query: Text to transform
    :return: Distance(s) and index(es) of the text
    """
    transformed_query = engine.transform(query)
    distances, indices = algorithm.kneighbors(transformed_query)
    return distances, indices


def result_category(similarity, confidence, resolution, new_balance, current_balance, payment_flag):
    """
    :param similarity: Similarity level (1 -100)
    :param confidence: confidence level (0 - 10)
    :param resolution: Predicted resolution
    :param new_balance: Predicted balance
    :param current_balance: Account balance
    :param payment_flag: Flag indicating if payments were posted
    :return: Category name
    """

    category = ''
    new_balance = float(new_balance)

    if similarity >= 90 and confidence <= 1.0:
        if resolution == 'PAID IN FULL' and new_balance == 0 and payment_flag == 1:
            category = 'UPDATE'

    if similarity >= 80 and confidence <= 1.0:
        if resolution == 'PAID IN FULL - BALANCE REPRESENTS PATIENT RESPONSIBILITY' and payment_flag == 1 \
                and 0 < new_balance <= current_balance:
            category = 'UPDATE'

        if resolution == 'BALANCE REPRESENTS PATIENT RESPONSIBILITY' and payment_flag == 0 \
                and 0 < new_balance <= current_balance:
            category = 'UPDATE'

    if category == '' and similarity >= 90 and confidence <= 1.0 and resolution == 'PAID IN FULL':
        category = 'PENDING REVIEW'

    if category == '' and similarity >= 80 and confidence <= 1.0 and resolution != 'PAID IN FULL':
        category = 'PENDING REVIEW'

    if category == '':
        category = 'LOW ACCURACY'

    return category


def main():

    export_dir = r'\\JZNVCS\VCS\Client_Exports\Jzanus\Machine_Learning\Cash_Posting'
    import_dir = r'\\JZNVCS\VCS\Client_Imports\Jzanus\Machine_Learning\Cash_Posting'
    train_dir = r'\\JZNVCS\VCS\Client_Exports\Jzanus\Machine_Learning\Cash_Posting\Train_Data'
    test_dir = r'\\JZNVCS\VCS\Client_Exports\Jzanus\Machine_Learning\Cash_Posting\Test_Data'
    train_file = ''
    log_dir = r'\\JZNVCS\VCS\Log_file'
    sql_server = 'JZNVCS'
    sql_database = 'VCS'

    # Start logging
    now = datetime.now()
    log_file = 'Cash_Posting_Prediction_' + now.strftime("%m%d%Y%H%M%S")
    initialize_logger(log_dir, log_file)

    # Check previous unprocessed files
    file_list = [f for f in os.listdir(export_dir) if f.endswith('.csv')]
    for f in file_list:
        if f.startswith('CP_Test'):
            shutil.move(os.path.join(export_dir, f), os.path.join(test_dir, f))
        else:
            shutil.move(os.path.join(export_dir, f), os.path.join(train_dir, f))

    # Create Files
    exec_sql(sql_server, sql_database, 'proc_ML_CashPosting_Export @tcType=?', 'TRAIN')
    time.sleep(6)

    # Create Files
    exec_sql(sql_server, sql_database, 'proc_ML_CashPosting_Export', '')
    time.sleep(6)

    # Find test files to process
    files_founds = [f for f in os.listdir(export_dir) if f.endswith('.csv') and f.startswith('CP_Test')]
    logging.info('{} test file(s) created!'.format(len(files_founds)))

    # Find latest training data set
    train_sets = [f for f in os.listdir(export_dir) if f.endswith('.csv') and f.startswith('CP_Train')]
    logging.info('{} train file(s) found!'.format(len(train_sets)))

    for data_set in train_sets:
        train_file = data_set

    # Load historical data
    logging.info('Loading and cleaning train data set...')
    historical_data = pd.read_csv(os.path.join(export_dir, train_file), sep='|', encoding='unicode_escape')
    historical_data['Note'] = historical_data['Note'].apply(lambda x: clean_note(x))
    historical_data['WordCounter'] = historical_data['Note'].apply(lambda x: check_keywords(x))
    train_data = historical_data[historical_data['WordCounter'] > 1]
    logging.info('Train data loaded and cleaned.')

    # Process each test file
    for file in files_founds:

        try:
            results_file = 'Results_' + file
            excel_report = 'Results_' + file.replace('.csv', '.xlsx')

            # Load test data
            logging.info('Loading and cleaning test data set...')
            new_data = pd.read_csv(os.path.join(export_dir, file), sep='|', encoding='unicode_escape')
            new_data['Note'] = new_data['Note'].apply(lambda x: clean_note(x))
            new_data['WordCounter'] = new_data['Note'].apply(lambda x: check_keywords(x))
            test_data = new_data[new_data['WordCounter'] >= 0]
            logging.info('Test data loaded and cleaned.')

            # Process test data using the K - model
            logging.info('Starting vectorization of the data...')

            historical_note = train_data['Note']
            engine = TfidfVectorizer(min_df=1, analyzer=get_n_grams, lowercase=False)
            model = engine.fit_transform(historical_note)

            logging.info('Vectorization completed.')

            algorithm = NearestNeighbors(n_neighbors=1, n_jobs=-1).fit(model)
            new_note = test_data['Note']
            account = test_data['AccountNo']
            trans = test_data['TransID']
            payment_flag = test_data['PaymentFlag']
            account_balance = test_data['AccountBalance']

            t1 = time.time()
            logging.info('Getting nearest n...')
            distances, indices = get_nearest_neighbor(engine, algorithm, new_note)
            t = time.time() - t1
            logging.info('Completed in: ' + str(t))

            logging.info('Finding matches...')
            new_note = list(new_note)
            account = list(account)
            trans = list(trans)
            payment_flag = list(payment_flag)
            account_balance = list(account_balance)

            matches = []
            for i, j in enumerate(indices):
                temp = [trans[i],
                        account[i],
                        new_note[i],
                        account_balance[i],
                        get_new_balance(train_data.values[j][0][2], train_data.values[j][0][1], new_note[i]),
                        train_data.values[j][0][1],
                        train_data.values[j][0][0],
                        train_data.values[j][0][2],
                        round(distances[i][0], 2),
                        fuzz.token_sort_ratio(new_note[i], train_data.values[j][0][1]),
                        payment_flag[i]]

                matches.append(temp)

            logging.info('Building results data set...')
            results = pd.DataFrame(matches, columns=['TransID',
                                                     'AccountNo',
                                                     'OriginalNote',
                                                     'AccountBalance',
                                                     'NewBalance',
                                                     'MatchedNote',
                                                     'MatchedResolution',
                                                     'MatchedBalance',
                                                     'MatchConfidence',
                                                     'SimilarityRatio',
                                                     'PaymentFlag'])

            results['ResultCategory'] = results.apply(lambda x: result_category(x['SimilarityRatio'],
                                                                                x['MatchConfidence'],
                                                                                x['MatchedResolution'],
                                                                                x['NewBalance'],
                                                                                x['AccountBalance'],
                                                                                x['PaymentFlag']),
                                                      axis=1)
            logging.info('Done')

            # Create results
            results.to_csv(os.path.join(import_dir, results_file), sep="|", index=False, quoting=csv.QUOTE_NONE)

            excel_df = results[['AccountNo',
                                'OriginalNote',
                                'MatchedNote',
                                'MatchedResolution',
                                'NewBalance',
                                'MatchConfidence',
                                'SimilarityRatio',
                                'ResultCategory']]

            create_excel(excel_df, os.path.join(import_dir, excel_report))

            logging.info(results_file + ' created!')
            logging.info(excel_report + ' created!')

            # Load results
            exec_sql(sql_server, sql_database, 'proc_ML_CashPosting_Import @tcFileName=?', results_file)

            # Move file to test data folder
            shutil.move(os.path.join(export_dir, file), os.path.join(test_dir, file))
            shutil.move(os.path.join(export_dir, train_file), os.path.join(train_dir, train_file))
            logging.info(file + ' has been process successfully!')

        except Exception as e:
            logging.error(str(e))


if __name__ == '__main__':
    main()
