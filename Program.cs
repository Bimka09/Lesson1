using System;
using Npgsql;
using Dapper;
using System.Data;
using System.Collections.Generic;

namespace Lesson1
{
    class Program
    {
        const string ConnectionString = "User ID=postgres;Password=k1t2i3f4;Host=localhost;Port=5432;Database=Ebay";
        public static List<string> tables = new List<string>(){ "clients", "products", "transactions"};

        static void Main(string[] args)
        {
            //CreateClientsTable();
            //Console.WriteLine(value: $"Created table by {nameof(CreateClientsTable)}.");
            //CreateProductsTable();
            //Console.WriteLine(value: $"Created table by {nameof(CreateProductsTable)}.");
            //CreateTransactionsTable();
            //Console.WriteLine(value: $"Created table by {nameof(CreateTransactionsTable)}.");
            //InsertClientsWithParams();
            //InsertProductsWithParams();
            //InsertTransactionsWithParams();
            //PrintTable();
            //AddData();
            do
            {
                Menu();
            } while (true);
        }
        static void CreateClientsTable()
        {
            using NpgsqlConnection connection = new NpgsqlConnection(connectionString: ConnectionString);
            connection.Open();

            string sql = @"
            CREATE SEQUENCE clients_id_seq;

            CREATE TABLE clients
            (
                id              BIGINT                      NOT NULL    DEFAULT NEXTVAL('clients_id_seq'),
                first_name      CHARACTER VARYING(255)      NOT NULL,
                last_name       CHARACTER VARYING(255)      NOT NULL,
                middle_name     CHARACTER VARYING(255),
                email           CHARACTER VARYING(255)      NOT NULL,
  
                CONSTRAINT clients_pkey PRIMARY KEY (id),
                CONSTRAINT clients_email_unique UNIQUE (email)
            );

            CREATE INDEX clients_last_name_idx ON clients(first_name);
            CREATE UNIQUE INDEX clients_email_unq_idx ON clients(lower(email));
            ";

            using NpgsqlCommand cmd = new NpgsqlCommand(cmdText: sql, connection: connection);

            string affectedRowsCount = cmd.ExecuteNonQuery().ToString();

            Console.WriteLine(value: $"Created CLIENTS table. Affected rows count: {affectedRowsCount}");
        }
        static void CreateProductsTable()
        {
            using NpgsqlConnection connection = new NpgsqlConnection(connectionString: ConnectionString);
            connection.Open();

            string sql = @"
            CREATE SEQUENCE products_id_seq;

            CREATE TABLE products
            (
                id              BIGINT                      NOT NULL    DEFAULT NEXTVAL('products_id_seq'),
                product         CHARACTER VARYING(255)      NOT NULL,
                customer_id     BIGINT                      NOT NULL,
  
                CONSTRAINT products_pkey PRIMARY KEY (id),
                CONSTRAINT products_fk_clients_id FOREIGN KEY (customer_id)  REFERENCES clients(id) ON DELETE CASCADE
            );

            CREATE INDEX product_idx ON products(product);

            ";

            using NpgsqlCommand cmd = new NpgsqlCommand(cmdText: sql, connection: connection);

            string affectedRowsCount = cmd.ExecuteNonQuery().ToString();

            Console.WriteLine(value: $"Created Products table. Affected rows count: {affectedRowsCount}");//DEFAULT CURRENT_TIMESTAMP(),
        }
        static void CreateTransactionsTable()
        {
            using NpgsqlConnection connection = new NpgsqlConnection(connectionString: ConnectionString);
            connection.Open();

            string sql = @"
            CREATE SEQUENCE transactions_id_seq;

            CREATE TABLE transactions
            (
                id              BIGINT      NOT NULL    DEFAULT NEXTVAL('transactions_id_seq'),
                customer_id     BIGINT      NOT NULL,
                product_id      BIGINT      NOT NULL,
                creation_date   TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,

  
                CONSTRAINT transactions_pkey PRIMARY KEY (id),
                CONSTRAINT transactions_fk_clients_id FOREIGN KEY (customer_id)  REFERENCES clients(id) ON DELETE CASCADE,
                CONSTRAINT transactions_fk_product_id FOREIGN KEY (product_id)  REFERENCES products(id) ON DELETE CASCADE
            );

            ";

            using NpgsqlCommand cmd = new NpgsqlCommand(cmdText: sql, connection: connection);

            string affectedRowsCount = cmd.ExecuteNonQuery().ToString();

            Console.WriteLine(value: $"Created Transactions table. Affected rows count: {affectedRowsCount}");
        }
        static void InsertClientsWithParams()
        {
            var firstNames = new List<string>(){ "Алексей", "Иван", "Петр", "Александр", "Эдуард", "Илья" };
            var lastNames = new List<string>(){ "Петров", "Александров", "Иванов", "Алексеев", "Ильин", "Эдуардов" };
            var middleNames = new List<string>(){ "Петрович", "Александрович", "Иванович", "Алексеевич", "Ильич", "Эдуардович" };
            var emails = new List<string>(){ "alex@gmail.com", "ivan@gmail.com", "petr@gmail.com", "alexandr@gmail.com", "eduard@gmail.com", "ilya@gmail.com" };

            using NpgsqlConnection connection = new NpgsqlConnection(connectionString: ConnectionString);
            connection.Open();

            string sql = @"
                    INSERT INTO clients(first_name, last_name, middle_name, email) 
                    VALUES (:first_name, :last_name, :middle_name, :email);
";

            using NpgsqlCommand cmd = new NpgsqlCommand(cmdText: sql, connection: connection);
            NpgsqlParameterCollection parameters = cmd.Parameters;
            for(int i =0; i < 6; i++)
            {
                parameters.Add(value: new NpgsqlParameter(parameterName: "first_name", value: firstNames[i]));
                parameters.Add(value: new NpgsqlParameter(parameterName: "last_name", value:  lastNames[i]));
                parameters.Add(value: new NpgsqlParameter(parameterName: "middle_name", value:  middleNames[i]));
                parameters.Add(value: new NpgsqlParameter(parameterName: "email", value:  emails[i]));

                string affectedRowsCount = cmd.ExecuteNonQuery().ToString();
                Console.WriteLine(value: $"Insert into CLIENTS table. Affected rows count: {affectedRowsCount}");
                parameters.Clear();
            }
        }
        static void InsertProductsWithParams()
        {
            var products = new List<string>() { "IPad", "IPhone", "Samsung", "Nokia", "Redmi", "Honor" };
            var customersID = new List<int>() { 1,2,3,4,5,6 };
            
            using NpgsqlConnection connection = new NpgsqlConnection(connectionString: ConnectionString);
            connection.Open();

            string sql = @"
                    INSERT INTO products(product, customer_id) 
                    VALUES (:product, :customer_id);
";

            using NpgsqlCommand cmd = new NpgsqlCommand(cmdText: sql, connection: connection);
            NpgsqlParameterCollection parameters = cmd.Parameters;
            for (int i = 0; i < 6; i++)
            {
                parameters.Add(value: new NpgsqlParameter(parameterName: "product", value: products[i]));
                parameters.Add(value: new NpgsqlParameter(parameterName: "customer_id", value: customersID[i]));


                string affectedRowsCount = cmd.ExecuteNonQuery().ToString();
                Console.WriteLine(value: $"Insert into CLIENTS table. Affected rows count: {affectedRowsCount}");
                parameters.Clear();
            }
        }
        static void InsertTransactionsWithParams()
        {
            var productsID = new List<int>() { 1, 2, 3, 4, 5, 6 };
            var customersID = new List<int>() { 1, 2, 3, 4, 5, 6 };

            using NpgsqlConnection connection = new NpgsqlConnection(connectionString: ConnectionString);
            connection.Open();

            string sql = @"
                    INSERT INTO transactions(customer_id, product_id) 
                    VALUES (:customer_id, :product_id);
";

            using NpgsqlCommand cmd = new NpgsqlCommand(cmdText: sql, connection: connection);
            NpgsqlParameterCollection parameters = cmd.Parameters;
            for (int i = 0; i < 6; i++)
            {
                parameters.Add(value: new NpgsqlParameter(parameterName: "product_id", value: productsID[i]));
                parameters.Add(value: new NpgsqlParameter(parameterName: "customer_id", value: customersID[i]));


                string affectedRowsCount = cmd.ExecuteNonQuery().ToString();
                Console.WriteLine(value: $"Insert into CLIENTS table. Affected rows count: {affectedRowsCount}");
                parameters.Clear();
            }
        }
        static void PrintTable()
        {
            int number = CheckInputTable();

            using NpgsqlConnection connection = new NpgsqlConnection(connectionString: ConnectionString);
            connection.Open();

            var sql = @"select * from " + tables[number - 1];
            using NpgsqlCommand cmd = new NpgsqlCommand(cmdText: sql, connection: connection);
            var reader = cmd.ExecuteReader();
            int count = 0;

            while (reader.Read())
            {
                count++;
                var row = new List<string>();
                for(int i=0; i <reader.FieldCount; i++)
                {
                    row.Add(reader[i].ToString());                    
                }
                Console.WriteLine($"{count} строка: {String.Join(",", row)}");
            }
            Console.WriteLine("Нажмите клавишу для продолжения работы.");
            Console.ReadKey();
        }
        static void AddData()
        {
            int number = CheckInputTable();

            using NpgsqlConnection connection = new NpgsqlConnection(connectionString: ConnectionString);
            connection.Open();

            var sql = @"SELECT * FROM " + tables[number - 1] + " LIMIT 1";
            using NpgsqlCommand cmd = new NpgsqlCommand(cmdText: sql, connection: connection);
            var reader = cmd.ExecuteReader();
            var parameters = new Dictionary<string, string>();

            while (reader.Read())
            {
                for (int i = 1; i < reader.FieldCount; i++)
                {
                    if(reader.GetFieldType(i).Name is not "DateTime")
                    {
                        Console.WriteLine($"Введите параметр для колонки {reader.GetName(i)} c типом {reader.GetFieldType(i)}:");
                        CheckInputParametr(reader.GetFieldType(i).Name, reader.GetName(i), parameters);
                    }
                      
                }
                break;
            }
            switch(number)
            {
                case 1:
                    InsertClients(parameters);
                    break;
                case 2:
                    InsertProducts(parameters);
                    break;
                case 3:
                    InsertTransactions(parameters);
                    break;
            }
            Console.WriteLine("Нажмите клавишу для продолжения работы.");
            Console.ReadKey();
        }
        static void InsertClients(Dictionary<string, string> data)
        {
            using NpgsqlConnection connection = new NpgsqlConnection(connectionString: ConnectionString);
            connection.Open();

            string sql = @"
                    INSERT INTO clients(first_name, last_name, middle_name, email) 
                    VALUES (:first_name, :last_name, :middle_name, :email);
";

            using NpgsqlCommand cmd = new NpgsqlCommand(cmdText: sql, connection: connection);
            NpgsqlParameterCollection parameters = cmd.Parameters;
            foreach(var element in data)
            {
                parameters.Add(value: new NpgsqlParameter(parameterName: element.Key, value: element.Value));
                
            }

            string affectedRowsCount = cmd.ExecuteNonQuery().ToString();
            Console.WriteLine(value: $"Insert into CLIENTS table. Affected rows count: {affectedRowsCount}");
        }
        static void InsertProducts(Dictionary<string, string> data)
        {
            using NpgsqlConnection connection = new NpgsqlConnection(connectionString: ConnectionString);
            connection.Open();

            string sql = @"
                    INSERT INTO products(product, customer_id) 
                    VALUES (:product, :customer_id);
";

            using NpgsqlCommand cmd = new NpgsqlCommand(cmdText: sql, connection: connection);
            NpgsqlParameterCollection parameters = cmd.Parameters;
            foreach (var element in data)
            {
                if(long.TryParse( element.Value, out _))
                {
                    parameters.Add(value: new NpgsqlParameter(parameterName: element.Key, value: Convert.ToInt64(element.Value)));
                }
                else
                {
                    parameters.Add(value: new NpgsqlParameter(parameterName: element.Key, value: element.Value));
                }
            }

            string affectedRowsCount = cmd.ExecuteNonQuery().ToString();
            Console.WriteLine(value: $"Insert into CLIENTS table. Affected rows count: {affectedRowsCount}");
        }
        static void InsertTransactions(Dictionary<string, string> data)
        {

            using NpgsqlConnection connection = new NpgsqlConnection(connectionString: ConnectionString);
            connection.Open();

            string sql = @"
                    INSERT INTO transactions(customer_id, product_id) 
                    VALUES (:customer_id, :product_id);
";

            using NpgsqlCommand cmd = new NpgsqlCommand(cmdText: sql, connection: connection);
            NpgsqlParameterCollection parameters = cmd.Parameters;
            foreach (var element in data)
            {
                if (long.TryParse(element.Value, out _))
                {
                    parameters.Add(value: new NpgsqlParameter(parameterName: element.Key, value: Convert.ToInt64(element.Value)));
                }
                else
                {
                    parameters.Add(value: new NpgsqlParameter(parameterName: element.Key, value: element.Value));
                }
            }

            string affectedRowsCount = cmd.ExecuteNonQuery().ToString();
            Console.WriteLine(value: $"Insert into CLIENTS table. Affected rows count: {affectedRowsCount}");
        }
        static long CheckInputTable()
        {
            Console.WriteLine($"Возможо добавление в следующие таблицы: {String.Join(", ", tables)}. Введите цифру интересной");
            long number = 1;
            bool checker = true;
            do
            {
                if (checker == false || (number > tables.Count || number < 1))
                    Console.WriteLine($"Некорректный ввод. Возможен от 1 до {tables.Count}");
                checker = long.TryParse(Console.ReadLine(), out number);

            } while (number > tables.Count || number < 1);

            return number;
        }
        static Dictionary<string, string> CheckInputParametr(string fieldType, string fieldName, Dictionary<string, string> parameters)
        {
            bool checker = false;
            string userInput;
            do
            {
                switch (fieldType)
                {
                    case "String":
                        parameters.Add(fieldName, Console.ReadLine());
                        checker = true;
                        break;
                    case "Int64":
                        userInput = Console.ReadLine();
                        checker = long.TryParse(userInput, out _);
                        if (checker == false)
                        {
                            Console.WriteLine($"Не удалось преобразовать параметр под тип {fieldType}. Повторите ввод:");
                        }
                        else
                        {
                            parameters.Add(fieldName, userInput);
                        }
                        break;
                }
            } while (checker == false);

            return parameters;
        }
        static void Menu()
        {
            Console.Clear();
            Console.WriteLine("-> Распечатать данные");
            Console.WriteLine("   Добавить данные");

            ConsoleKeyInfo line;
            int column = 0;
            Console.SetCursorPosition(0, 0);
            do
            {
                line = Console.ReadKey();
                if (line.Key == ConsoleKey.UpArrow)
                {
                    if (column != 0)
                    {
                        column -= 1;
                        Console.SetCursorPosition(0, Console.GetCursorPosition().Top);
                        Console.Write("   "); 
                        Console.SetCursorPosition(0, column);
                        Console.Write("-> ");
                    }
                }

                if (line.Key == ConsoleKey.DownArrow)
                {
                    if (column != 1)
                    {
                        column += 1;
                        Console.SetCursorPosition(0, Console.GetCursorPosition().Top);
                        Console.Write("   ");
                        Console.SetCursorPosition(0, column);
                        Console.Write("-> ");
                    }
                }

            } while (line.Key != ConsoleKey.Enter);

            switch(column)
            {
                case 0:
                    Console.Clear();
                    PrintTable();
                    Console.Clear();
                    break;
                case 1:
                    Console.Clear();
                    AddData();
                    Console.Clear();
                    break;
            }

        }
    }
}
