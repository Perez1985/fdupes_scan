/*
Программа анализирует входной файл с дубликатами файлов. На основании заранее
определенных условий программа формирует набор файлов для удаления.
Хочу также реализовать подсчет времени на загрузку, анализ, на запись в файл и
суммарного времени выполнения.
*/

/*
v0.2 - правила загружаюся из внешнего файла fdupes_scan.conf
*/
#define _GNU_SOURCE // для работы getline обозначение должно быть до #include <stdio.h> иначе вылетает ошибка
#include <stdio.h>
#include <stdbool.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <confuse.h>

#include <sys/resource.h>
#include <sys/time.h>

#define DUPES_MAX 500000

#undef calculate
#undef getrusage
/*
TODO

* Написать функцию добавления узла в структуру dupes, узел должен добавляться
в определенное место связанного списка структуры - список должен быть
отсортирован по пути.

* функция очистки памяти от dupes

*/

/*
Определяю структуру для хранения путей дубликатов файлов
*/
typedef struct dupes_file // определяю структуру для связанного списка
{
    char* path; // в структуре храниться путь к файлу
    char status; // метка для обозначения статуса файла [n|k|r] отсуствует - сохранить - удалить
    struct dupes_file* next; // также в структуре хранится ссылка на следующий узел
} dupes_file; // тут определяется тип для струтуры

typedef struct Opt// структура для хранения перечня опций
{
    char* conf; // имя конфигурационного файла
    char* in_file; // имя файла для анализа
} Opt;

Opt* opt; // инициализируем переменную для храения опций


cfg_t *cfg; // инициализируем переменную для хранения данных файла конфигурации

dupes_file* dupes[DUPES_MAX]; /* создаю глобальную переменную неопределенной
длины(из-за ошибки компилятора пришлось задать значение 500000) для хранения
ссылок на дубликаты */

dupes_file* rm_dupes; /* создаю глобальную переменную связонного списка
удаляемых файлов */

unsigned long dupes_num = 0; // инициализируем переменную числа файлов-дубликатов

/**
 * Returns number of seconds between b and a.
 */
double calculate(const struct rusage* b, const struct rusage* a)
{
    if (b == NULL || a == NULL)
    {
        return 0.0;
    }
    else
    {
        return ((((a->ru_utime.tv_sec * 1000000 + a->ru_utime.tv_usec) -
                 (b->ru_utime.tv_sec * 1000000 + b->ru_utime.tv_usec)) +
                ((a->ru_stime.tv_sec * 1000000 + a->ru_stime.tv_usec) -
                 (b->ru_stime.tv_sec * 1000000 + b->ru_stime.tv_usec)))
                / 1000000.0);
    }
}

void help_message(void)
{
    printf("Error. Wrong usage. Use: ./fdupes_scan -i input_file_name -c conf_file_name | sort > output_file_name\n");
}

/*
фукнция проверяет введерные значения аргументов на предмет коррекности. В 
случае если аргументы не коректны возвращает значение ложь. Если аргументы
коректны - то возвращает правду.
В качестве первого аргумента ожидается имя файла с перечнем дубликатов файлов
В качестве второго аргумента ожидается имя файла конфигурации
Проверяем: есть ли аргрумент, один ли он, существует ли входной файл.
*/
bool initial_parse(int argc, char* argv[]) // todo: debug
{
    opt = malloc(sizeof(Opt));
    /*
    проверяем наличие лишь двух аргуметов и факт того что файл из первого аргумента существует
    */
    int option = 0;
    while((option = getopt (argc, argv, "i:c:")) != -1) // обработка ключей запуска
    {
        switch (option)
        {
        case 'c':
            opt->conf = optarg;
            break;
        case 'i':
            /*opt->in_file = malloc(sizeof(optarg) + 1);
            strcpy(opt->in_file, optarg);// очень сложно, можно проще. ниже проще*/
            opt->in_file = optarg;
            break;
        }
    }
    if(access(opt->in_file, F_OK) != -1 && access(opt->conf, F_OK) != -1)
    {
        return true;
    }
    else
    {
        help_message();
        return false;
    }
}

/*
Функция для чтения конфигурационного файла
*/
bool conf_file_read(char* file_path) // todo: make, debug
{
    /*
    определям структуру ожидаемого файла опций
    */
    cfg_opt_t rule_opts[] = 
    {
		CFG_STR("path", 0, CFGF_NONE),
		CFG_BOOL("keep", cfg_false, CFGF_NONE),
		CFG_END()
    };
    
	cfg_opt_t backup_dir_opts[] = 
	{
		CFG_SEC("rule", rule_opts, CFGF_MULTI | CFGF_TITLE),
		CFG_END()
	};
	
	cfg_opt_t opts[] = 
	{
		CFG_SEC("backup_dir", backup_dir_opts, CFGF_MULTI | CFGF_TITLE),
		CFG_END()
	};
	
	cfg = cfg_init(opts, CFGF_NOCASE); // инициализируем переменную для хранения опций
	if(cfg_parse(cfg, file_path) == CFG_PARSE_ERROR)
	{
	    printf("Error of parsing config file.\n");
	    return false;
	}
	return true;
}

/*
Функция предусматривается для загрузки файла в память. В случае успеха загрузки
возвращает значение истина. В случае неудачи загрузки выдает значение ложь.
*/
bool input_file_read(char* file_path)
{
    unsigned int dupes_max = 0;
    FILE* in_fp = fopen(file_path, "r"); //открываю файл на чтение
    if (in_fp == NULL) // проверяю если файл не открылся то возвращаю ошибку
    {
        printf("Error. Cannot read input file\n");
        return false;
    }
    char* temp_line = NULL; // инициализирую переменную временного слова и присваиваю ее в NULL
    size_t len = 0; // переменная для функции построчного чтения
    ssize_t read_len; // переменная для функции построчного чтения
    for (int i = 0; i < DUPES_MAX; i++)
    {
        dupes[i] = malloc(sizeof(dupes_file));
    }
    dupes[0]->path = NULL;
    dupes[0]->next = NULL;
    dupes[0]->status = 'n';
    unsigned int dupes_max_temp = 0;
    while((read_len = getline(&temp_line, &len, in_fp)) != -1) // читаем по одной строке до конца файла
    {
        if(strcmp(temp_line, "\r\n") == 0 || strcmp(temp_line, "\n") == 0) // проверяем случай, когда строка равна переносу строки
        {
            if (dupes_max < dupes_max_temp) dupes_max = dupes_max_temp;
            dupes_num++;
            dupes_max_temp = 0;
            dupes[dupes_num]->path = NULL;
            dupes[dupes_num]->next = NULL;
            dupes[dupes_num]->status = 'n';
        }
        else
        {
            if (dupes[dupes_num]->path == NULL)
            {
                dupes[dupes_num]->path = malloc(read_len + 1);
                strcpy(dupes[dupes_num]->path, temp_line);
                dupes_max_temp++;
            }
            else
            {
                dupes_file* pointer = dupes[dupes_num];
                dupes_file* pointer_back = NULL;
                
                while(strcmp(pointer->path, temp_line) < 0 && pointer->next != NULL)
                /* проверяю факт того что новое слово по алфавиту ниже слова в узле 
                или что узел последний в связанном списке */
                {
                    pointer_back = pointer;
                    pointer = pointer->next;
                }
                
                dupes_file* dupes_temp = malloc (sizeof(dupes_file)); /* создаем новый узел структуры для хранения
                ссылки на следующий и ссылки на файл */
                dupes_temp->path = malloc(read_len + 1);
                strcpy(dupes_temp->path, temp_line); /* копируем содержимое прочитанной строки из файла
                в новый узел в памяти */
                dupes_temp->status = 'n';
                if (strcmp(pointer->path, temp_line) >= 0) // случай, когда вставляем перед pointer (сортировка)
                {
                    dupes_temp->next = pointer; // новый узел ссылаем на текущий
                    if (pointer_back != NULL)
                    {
                        pointer_back->next = dupes_temp; // предыдущий узел ссылаем на новый
                    }
                    else
                    {
                        dupes[dupes_num] = dupes_temp;
                    }
                }
                else // если отсутсвует ссылка на следующий узел
                {
                    dupes_temp->next = NULL; /* ссылке на следующий узел нового узла присваиваем значение NULL */
                    pointer->next = dupes_temp; // предыдущий узел ссылаем на новый
                }
                dupes_max_temp++;
            }
        }
    }
    printf("Result: 1: Number of duplicates files: %lu\n", dupes_num+1);
    printf("Result: 2: Max number of duplicates of one file is: %u\n", dupes_max);
    return true;
}


/*
Функция для определения необходимости хранения файла на основании заранее
определенных масок
*/
bool keep_file(char* path)
{
    /*if (path == NULL) return false;
    if (strstr(path, "/mnt/hdd_work/backup_S/") != NULL && 
        strstr(path, "/mnt/hdd_work/backup_S/TEMP/") == NULL) return true;
    else return false;*/
    
    if (path == NULL) return false; // если путь недействителен - возвращаем неправду
    bool keep = false; // инициализируем локальную переменную для работы с правилами внутри конфигурационного файла
    bool catch_str = false; // инициализируем переменную для сигнализации первого совпадения пути, для ясности к какому блоку backup_dir относиться путь
    unsigned int BackupDirNumber = cfg_size(cfg, "backup_dir"); // получаем кол-во обрабатываемых директорий в файле конфигурации
    for (int i = 0; i < BackupDirNumber; i++) // организовываем цикл, где обрабатываем каждый каталог в файле конфигурации
    {
        cfg_t *BD = cfg_getnsec(cfg, "backup_dir", i); // создаем локальную переменную для загрузки в память правил для обрабатываемой ветки конфигурации
        unsigned int RuleNumber = cfg_size(BD, "rule"); // получаем кол-во правил в обрабатываемой ветке конфигурации
        for (int j = 0; j < RuleNumber; j++) // огранизовываем цикл, где обрабатываем каждое правило в ветке конфигурации
        {
            cfg_t *RL = cfg_getnsec(BD, "rule", j); // создаем локальную переменную для загрузки в память отдельного правила в ветке конфигурации
            /* эта часть кода ниже нужна была для проверки работы функции
            char* rl_path = cfg_getstr(RL, "path");
            printf("%s",rl_path);
            bool rl_keep = cfg_getbool(RL, "keep");
            if (rl_keep) printf("rl_keep is true");
            */
            if (strstr(path, cfg_getstr(RL, "path")) != NULL)
            // если есть совпадение со строкой из файла конфигурации - значит файл относиться к этому блоку
            {
                catch_str = catch_str || true;
                keep = true;
            }
            if ((strstr(path, cfg_getstr(RL, "path")) != NULL) == cfg_getbool(RL, "keep"))
            /* проверяем есть ли в в проверяемом пути путь из правила в файле конфигурации
            как часть строки, и проверяем факт того должен ли там этот путь быть */
            {
                keep = keep && true;
            }
            else
            {
                keep = false;
            }
        }
        if (catch_str)
        {
            return keep;
        }
    }
    return keep;
}

/* Функция удаления узла из связанного списка */
void struct_del(dupes_file* in_pointer, dupes_file* struct_dupes)
{
    dupes_file* pointer = struct_dupes;
    dupes_file* pointer_back;
    bool is_deleted = false;
    if (in_pointer == struct_dupes)
    {
        struct_dupes = struct_dupes->next;
    }
    do
    {
        pointer_back = pointer;
        pointer = pointer->next;
        if (pointer == in_pointer)
        {
            pointer_back->next = pointer->next;
            is_deleted = true;
        }
        
    }
    while(!is_deleted && pointer->next != NULL);
    /* проверяю факт того что новое слово по алфавиту ниже слова в узле 
    или что узел последний в связанном списке */
}

/* Функция добавления узла в связанный список */
void struct_add(dupes_file* in_pointer)
/* Первый вариант реализации данной функции:
Вариант подразумевал сортировку данных при добавлении в связанный список rm_dupes.
При реализации возникли следующие проблема - поскольку для сортировки подразумевается анализ
данных, а связанный список не имеет прозвольного доступа, то для каждой операции добавления
нового узла приходится проходить весь список до точки вставки. При определенной длине списка
(50%) от требуемых входных данных добавление каждого узла происходит настолько медленно, что
анализ занимает более двух часов. Принято решение перейти к несортированному списку для ускорения
процесса, далее сортировать посредством утилиты sort. Также альтернативным вариантом могут стать
функции хеширования, что позволит в N раз сократить длину связанного списка, где N - кол-во 
вариантов хеш функции. Однако при таком подходе придется существенно усложнить функционал записи
данных в выходной файл, т.к. придется "сливать в один котел" из N сортированых списков.
{
    dupes_file* pointer = rm_dupes;
    dupes_file* pointer_back = NULL;
    dupes_file* new_rm_dupe = malloc(sizeof(dupes_file));
    new_rm_dupe->path = malloc (strlen(in_pointer->path)+1);
    strcpy(new_rm_dupe->path, in_pointer->path);
    
    if (pointer->path == NULL)
    {
        new_rm_dupe->next = NULL;
        rm_dupes = new_rm_dupe;
    }
    else
    {
        while(strcmp(pointer->path, in_pointer->path) < 0 && pointer->next != NULL)
        // проверяю факт того что новое слово по алфавиту ниже слова в узле или что узел последний в связанном списке
        {
            pointer_back = pointer;
            pointer = pointer->next;
        }
        
        if (strcmp(pointer->path, in_pointer->path) >= 0) // случай, когда вставляем перед pointer (сортировка)
        {
            new_rm_dupe->next = pointer; // новый узел ссылаем на текущий
            if (pointer_back != NULL)
            {
                pointer_back->next = new_rm_dupe; // предыдущий узел ссылаем на новый
            }
            else
            {
                rm_dupes = new_rm_dupe;
            }
        }
        else // если отсутсвует ссылка на следующий узел
        {
            new_rm_dupe->next = NULL; // ссылке на следующий узел нового узла присваиваем значение NULL
            pointer->next = new_rm_dupe; // предыдущий узел ссылаем на новый
        }
    }
}
*/

/* Вариант второй - без соритровки. Новый узел просто добавляется в начало списка.*/
{
    dupes_file* new_rm_dupe = malloc(sizeof(dupes_file));
    new_rm_dupe->path = malloc (strlen(in_pointer->path)+1);
    strcpy(new_rm_dupe->path, in_pointer->path);
    if (rm_dupes->path == NULL)
    {
        new_rm_dupe->next = NULL;
        rm_dupes = new_rm_dupe;
    }
    else
    {
        new_rm_dupe->next = rm_dupes;
        rm_dupes = new_rm_dupe;
    }
}




/*
Фунция анализирует стуктуру в памяти и формирует набор файлов для удаления.
Файлы для удаления организуются в связанный список. В процессе работы со 
списком файлы сортируются.
*/
bool dupes_scan(dupes_file** struct_dupes) //todo: dubug
{
    rm_dupes = malloc(sizeof(dupes_file));
    for (unsigned long i = 0; i <= dupes_num; i++)
    /* анализ и простановка статусов во всех узлах */
    {
        // предварительный перебор
        dupes_file* pointer = struct_dupes[i];
        bool is_keeped = false; //переменная для сигнализации того, что хотябы одна копия файла сохранена
        if (keep_file(pointer->path) && pointer->status == 'n')
        //bool temp = keep_file(pointer->path);
        //if (temp && pointer->status == 'n')
        /* проверяю что путь соотвествует шаблону сохранения, и то что
        статус пока не назначен */
        {
            pointer->status = 'k'; // присваиваю статус "сохранить"
            is_keeped = true;
        }
        do
        {
            if (pointer->next != NULL)
            /* если ссылка на следующий узел не пустая */
            {
                pointer = pointer->next;
                /* присвоить значение узлу следующему узлу по ссылке */
            }
            if (keep_file(pointer->path) && pointer->status == 'n')
            /* проверяю что путь соотвествует шаблону сохранения, и то что
            статус пока не назначен */
            {
                pointer->status = 'k'; // присваиваю статус "сохранить"
                is_keeped = true;
            }
        }
        while(pointer->next != NULL);
        
        // перебор для простановки всех статусов
        pointer = struct_dupes[i];
        if (is_keeped)
        {
            if (pointer->status == 'n')
            {
                pointer->status = 'r'; // присваиваю статус "удалить"
            }
        }
        else
        {
            if (struct_dupes[i]->status == 'n')
            {
                struct_dupes[i]->status = 'k';
            }
            else if (struct_dupes[i]->status == 'k' && pointer != struct_dupes[i])
            {
                pointer->status = 'r'; // присваиваю статус "удалить"
            }
        }
        do
        {
            if (pointer->next != NULL)
            /* если ссылка на следующий узел не пустая */
            {
                pointer = pointer->next;
                /* присвоить значение узлу следующему узлу по ссылке */
            }
            
            if (is_keeped)
            {
                if (pointer->status == 'n')
                {
                    pointer->status = 'r'; // присваиваю статус "удалить"
                }
            }
            else
            {
                if (struct_dupes[i]->status == 'n')
                {
                    struct_dupes[i]->status = 'k';
                }
                else if (struct_dupes[i]->status == 'k' && pointer != struct_dupes[i])
                {
                    pointer->status = 'r'; // присваиваю статус "удалить"
                }
            }
        }
        while (pointer->next != NULL);
        
        //формирование стуктур удаления и оставшейся структуры
        pointer = struct_dupes[i];
        if (pointer->status == 'r')
        {
            struct_add(pointer);
            struct_del(pointer, struct_dupes[i]);
        }
        else if (pointer->status == 'n')
        {
            return false;
        }
        
        do
        {
            if (pointer->next != NULL)
            /* если ссылка на следующий узел не пустая */
            {
                pointer = pointer->next;
                /* присвоить значение узлу следующему узлу по ссылке */
            }
            
            if (pointer->status == 'r')
            {
                struct_add(pointer);
                struct_del(pointer, struct_dupes[i]);
            }
            else if (pointer->status == 'n')
            {
                return false;
            }
            
        }
        while (pointer->next != NULL);
    }
    return true;
}

/*
Функция реализует запись в файл перечня файлов для удаления. В случае успеха
возвращает истину. В случае неудачи - ложь.
*/
void out_file_write(void) // todo: debug, make
{
    dupes_file* pointer = rm_dupes;
    printf("%s", pointer->path);
    do
    {
        if (pointer->next != NULL)
        /* если ссылка на следующий узел не пустая */
        {
            pointer = pointer->next;
            /* присвоить значение узлу следующему узлу по ссылке */
            printf("%s", pointer->path);
        }
    }
    while (pointer->next != NULL);
}


/*
Функция реализует очистку памяти от структур
*/
bool unload(dupes_file* struct_dupes) // todo: debug, make
{
    free(struct_dupes);
    return true;
}

int main(int argc, char* argv[])
{
    if (initial_parse(argc, argv) && conf_file_read(opt->conf)) // производим проверки инициализации 
    {
        // structs for timing data
        struct rusage before, after;
        
        // benchmarks
        double time_read = 0.0, time_scan = 0.0, time_write = 0.0, time_unload = 0.0;
        
        getrusage(RUSAGE_SELF, &before);
        if (!input_file_read(opt->in_file)) // загрузка данных в память
        {
            return 1;
        }
        getrusage(RUSAGE_SELF, &after);
        time_read = calculate(&before, &after);
        
        getrusage(RUSAGE_SELF, &before);
        if (!dupes_scan(dupes)) // анализ данных
        {
            return 1;
        }
        getrusage(RUSAGE_SELF, &after);
        time_scan = calculate(&before, &after);
        
        getrusage(RUSAGE_SELF, &before);
        out_file_write(); // запись перечня файлов для удаления в выходной файл
        getrusage(RUSAGE_SELF, &after);
        time_write = calculate(&before, &after);
        
        getrusage(RUSAGE_SELF, &before);
        if (!unload(rm_dupes)) // очистка памяти
        {
            return 1;
        }
        for (unsigned long i = 0 ; i < dupes_num; i++)
        {
            if (!unload(dupes[i])) // очистка памяти
            {
                return 1;
            }
        }
        getrusage(RUSAGE_SELF, &after);
        time_unload = calculate(&before, &after);
        
        // report benchmarks
        printf("Result: 3: TIME IN read:         %.2f\n", time_read);
        printf("Result: 4: TIME IN scan:         %.2f\n", time_scan);
        printf("Result: 5: TIME IN write:        %.2f\n", time_write);
        printf("Result: 6: TIME IN unload:       %.2f\n", time_unload);
        printf("Result: 7: TIME IN TOTAL:        %.2f\n\n", 
         time_read + time_scan + time_write + time_unload);
        
        return 0;
    }
    else
    {
        return 1;
    }
}
