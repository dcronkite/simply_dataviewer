import pandas as pd
import pathlib
import tkinter as tk
from tkinter import filedialog, messagebox


class TextBoxVariable:

    def __init__(self, text_box):
        self.window = text_box

    def set(self, value):
        self.window.delete(1.0, tk.END)
        self.window.insert(1.0, value)


class Application(tk.Frame):

    def __init__(self, master=None):
        super().__init__(master)
        self.master = master
        self.pack(fill=tk.BOTH, expand=1)

        # variables
        self.saved_project_to_data = {}  # name-date string => uid, name, last_viewed_index
        self.current_file = None
        self.chunksize = 10 ** 3
        self.current_df = None
        self.path: pathlib.Path = None
        self.reader = None
        self.records = []
        self.records_index = 0
        self.queue_index = 0
        self.response_rows = 26

        # create widgets
        self.create_widgets()
        self.extra_widgets = []  # widgets created from each row
        self.col_to_widget_text = {}

    def create_widgets(self):
        """Layout header widgets"""
        self.btn_prev = tk.Button(self, text='Previous Record', command=self.get_previous_record)
        self.btn_prev.grid(row=1, column=1)
        self.doc_path_label = tk.Label(self, text='Path')
        self.doc_path_label.grid(row=1, column=2)
        self.doc_path_entry = tk.Entry(self, width=100, textvariable=self.path)
        self.doc_path_entry.grid(row=1, column=3, columnspan=4)
        self.btn_browse_folder = tk.Button(self, text="Get File to Review", command=self.get_file_to_review)
        self.btn_browse_folder.grid(row=1, column=7)
        self.btn_next = tk.Button(self, text='Next Record', command=self.get_next_record)
        self.btn_next.grid(row=1, column=8)

    def _clear_widgets(self):
        for widget in self.extra_widgets:
            widget.destroy()
        self.extra_widgets = []

    def get_file_to_review(self):
        path = filedialog.askopenfilename(title='Select a File', filetypes=(('All files', '*.*'),
                                                                            ('CSV files', '*.csv*'),
                                                                            ('SAS files', '*.sas7bdat'),
                                                                            ('Excel files', '*.xls*')), )
        if not path:
            return
        self._load_file(path.strip(' \'"'))

    def _load_file(self, path):
        self._clear_widgets()
        self.path = pathlib.Path(path)
        if self.path.suffix == '.csv':
            self.reader = pd.read_csv(self.path, chunksize=self.chunksize, engine='python')
        elif self.path.suffix in {'.xls', '.xlsx'}:
            self.reader = iter([pd.read_excel(self.path)])  # pretend to chunk
        elif self.path.suffix in {'.sas7bdat'}:
            self.reader = pd.read_sas(self.path, chunksize=self.chunksize)
        else:
            self.show_message(f'Unrecognized File',
                              f'Expected "csv", "xls", "xlsx", or "sas7bdat"; but got "{self.path.suffix}"')
            return
        self.records_index = 0
        self.current_df = next(self.reader)
        self._load_row()

    def _load_row(self):
        """
        Load individual row.

        FIX: these elements are laid out and can just be filled in subsequently
        :return:
        """
        self._clear_widgets()  # probably should just fill in existing widgets
        current_record = self.current_df.iloc[self.records_index]
        fieldnames = set(self.current_df.columns)
        row_number = 2
        column_number = 1
        for col in fieldnames:
            val = current_record[col]
            length = len(str(val))
            if length < 65:  # don't use text box
                var = tk.StringVar()
                label = tk.Label(self, text=col)
                if length < 15:
                    label.grid(row=row_number, column=column_number)
                    entry = tk.Entry(self, width=20, textvariable=var)
                    entry.grid(row=row_number, column=column_number + 1, columnspan=1)
                    column_number += 3
                elif length < 25:
                    label.grid(row=row_number, column=column_number)
                    entry = tk.Entry(self, width=30, textvariable=var)
                    entry.grid(row=row_number, column=column_number + 1, columnspan=2)
                    column_number += 4
                else:
                    label.grid(row=row_number, column=column_number)
                    entry = tk.Entry(self, width=70, textvariable=var)
                    entry.grid(row=row_number, column=column_number + 1, columnspan=3)
                    column_number += 5
            else:  # this should be a text box
                label = tk.Label(self, text=col)
                row_number += 1
                column_number = 1
                label.grid(row=row_number, column=column_number)
                if length < 140:
                    entry = tk.Text(self, width=140, height=2)
                elif length < 300:
                    entry = tk.Text(self, width=140, height=5)
                elif length < 1000:
                    entry = tk.Text(self, width=140, height=10)
                elif length < 2500:
                    entry = tk.Text(self, width=140, height=15)
                else:
                    entry = tk.Text(self, width=140, height=30)
                row_number += 1
                entry.grid(row=row_number, column=column_number, columnspan=10)
                row_number += 1
                var = TextBoxVariable(entry)
            # update for all
            self.extra_widgets += [label, entry]
            self.col_to_widget_text[col] = var
            var.set(val)
            if column_number > 8:
                row_number += 1
                column_number = 1

    def _get_other_columns(self):
        for col, textvar in self.col_to_widget_text.items():
            val = self.current_df.get(col, '*MISSING*')
            textvar.set(val)

    def show_message(self, title, msg):
        messagebox.showerror(title, msg)

    def get_previous_record(self):
        """
        Display the previous record
        :return:
        """
        if self.reader is None:  # no file prepared
            return
        self.records_index -= 1
        if self.records_index < 0:  # FIX: need to go back to previous dataframe if on dataframe > 0
            self._set_last_chunk()  # get the last record
        self._load_row()

    def get_next_record(self):
        """
        Get next record in dataframe/file
        Handle:
        1. Last record in file
        2. Last record in current chunk
        :return:
        """
        if self.reader is None:
            return  # no file selected
        self.records_index += 1  # next row
        if self.records_index >= self.current_df.shape[-1]:
            # move to next dataframe
            try:
                self.current_df = next(self.reader)  # last record in current chunk, get next chunk
            except StopIteration as e:
                return self._load_file(self.path)  # last record in file, re-open file
        self._load_row()

    def _set_last_chunk(self):
        """Get the last row"""
        df = None
        for df in self.reader:
            pass
        if df is not None:
            self.current_df = df
        self.records_index = self.current_df.shape[-1] - 1

    def close(self):
        """Handle exiting app"""
        pass  # nothing to do here now


def on_closing():
    app.close()
    root.destroy()


root = tk.Tk()
root.title('Simply DataViewer')
root.geometry('1400x1400')
root.protocol('WM_DELETE_WINDOW', on_closing)
app = Application(master=root)
app.mainloop()
