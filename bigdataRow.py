class BigdataRow:
  client_id = ''
  trans_arr= []


  def __init__(self, client_id, trans_arr):
    self.client_id = client_id
    self.trans_arr = trans_arr



class Transaction:
  def __init__(self, trans_id, trans_date, terminal_address):
    self.trans_id = trans_id
    self.trans_date = trans_date
    self.terminal_address = terminal_address
