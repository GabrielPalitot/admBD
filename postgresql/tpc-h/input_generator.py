import random
from datetime import date, timedelta

# Listas baseadas na especificação (Clause 4.2.2.13 e 4.2.3)
NATIONS = [
    'ALGERIA', 'ARGENTINA', 'BRAZIL', 'CANADA', 'EGYPT', 'ETHIOPIA', 'FRANCE', 
    'GERMANY', 'INDIA', 'INDONESIA', 'IRAN', 'IRAQ', 'JAPAN', 'JORDAN', 'KENYA', 
    'MOROCCO', 'MOZAMBIQUE', 'PERU', 'CHINA', 'ROMANIA', 'SAUDI ARABIA', 
    'VIETNAM', 'RUSSIA', 'UNITED KINGDOM', 'UNITED STATES'
]

REGIONS = ['AFRICA', 'AMERICA', 'ASIA', 'EUROPE', 'MIDDLE EAST']

# Mapeamento de nação para região para a Query 8
NATION_TO_REGION = {
    'ALGERIA': 'AFRICA', 'ETHIOPIA': 'AFRICA', 'KENYA': 'AFRICA', 'MOROCCO': 'AFRICA', 'MOZAMBIQUE': 'AFRICA',
    'ARGENTINA': 'AMERICA', 'BRAZIL': 'AMERICA', 'CANADA': 'AMERICA', 'PERU': 'AMERICA', 'UNITED STATES': 'AMERICA',
    'INDIA': 'ASIA', 'INDONESIA': 'ASIA', 'JAPAN': 'ASIA', 'CHINA': 'ASIA', 'VIETNAM': 'ASIA',
    'FRANCE': 'EUROPE', 'GERMANY': 'EUROPE', 'ROMANIA': 'EUROPE', 'RUSSIA': 'EUROPE', 'UNITED KINGDOM': 'EUROPE',
    'EGYPT': 'MIDDLE EAST', 'IRAN': 'MIDDLE EAST', 'IRAQ': 'MIDDLE EAST', 'JORDAN': 'MIDDLE EAST', 'SAUDI ARABIA': 'MIDDLE EAST'
}

P_NAME_COLORS = [
    "almond", "antique", "aquamarine", "azure", "beige", "bisque", "black", "blanched", "blue",
    "blush", "brown", "burlywood", "burnished", "chartreuse", "chiffon", "chocolate", "coral",
    "cornflower", "cornsilk", "cream", "cyan", "dark", "deep", "dim", "dodger", "drab", "firebrick",
    "floral", "forest", "frosted", "gainsboro", "ghost", "goldenrod", "green", "grey", "honeydew",
    "hot", "indian", "ivory", "khaki", "lace", "lavender", "lawn", "lemon", "light", "lime", "linen",
    "magenta", "maroon", "medium", "metallic", "midnight", "mint", "misty", "moccasin", "navajo",
    "navy", "olive", "orange", "orchid", "pale", "papaya", "peach", "peru", "pink", "plum", "powder",
    "puff", "purple", "red", "rose", "rosy", "royal", "saddle", "salmon", "sandy", "seashell", "sienna",
    "sky", "slate", "smoke", "snow", "spring", "steel", "tan", "thistle", "tomato", "turquoise", "violet",
    "wheat", "white", "yellow"
]

SEGMENTS = ['AUTOMOBILE', 'BUILDING', 'FURNITURE', 'MACHINERY', 'HOUSEHOLD']
MODES = ['REG AIR', 'AIR', 'RAIL', 'SHIP', 'TRUCK', 'MAIL', 'FOB']
CONTAINERS_S1 = ['SM', 'LG', 'MED', 'JUMBO', 'WRAP']
CONTAINERS_S2 = ['CASE', 'BOX', 'BAG', 'JAR', 'PKG', 'PACK', 'CAN', 'DRUM']
TYPES_S1 = ['STANDARD', 'SMALL', 'MEDIUM', 'LARGE', 'ECONOMY', 'PROMO']
TYPES_S2 = ['ANODIZED', 'BURNISHED', 'PLATED', 'POLISHED', 'BRUSHED']
TYPES_S3 = ['TIN', 'NICKEL', 'BRASS', 'STEEL', 'COPPER']
WORDS_1 = ['special', 'pending', 'unusual', 'express']
WORDS_2 = ['packages', 'requests', 'accounts', 'deposits']

# Funções auxiliares
def random_date(start_date, end_date):
    """Gera uma data aleatória no formato YYYY-MM-DD."""
    time_between_dates = end_date - start_date
    days_between_dates = time_between_dates.days
    random_number_of_days = random.randrange(days_between_dates)
    random_dt = start_date + timedelta(days=random_number_of_days)
    return random_dt.strftime("%Y-%m-%d")


def get_q1_inputs():
    return {'DELTA': random.randint(60, 120)}


def get_q2_inputs():
    return {
        'SIZE': random.randint(1, 50),
        'TYPE': random.choice(TYPES_S3),
        'REGION': random.choice(REGIONS)
    }


def get_q3_inputs():
    return {
        'SEGMENT': random.choice(SEGMENTS),
        'DATE': random_date(date(1995, 3, 1), date(1995, 3, 31))
    }

def get_q4_inputs():
    year = random.randint(1993, 1997)
    # Limita o mês para 1997
    month = random.randint(1, 10) if year == 1997 else random.randint(1, 12)
    return {'DATE': date(year, month, 1).strftime("%Y-%m-%d")}

def get_q5_inputs():
    return {
        'REGION': random.choice(REGIONS),
        'DATE': date(random.randint(1993, 1997), 1, 1).strftime("%Y-%m-%d")
    }

def get_q6_inputs():
    return {
        'DATE': date(random.randint(1993, 1997), 1, 1).strftime("%Y-%m-%d"),
        'DISCOUNT': f"{random.randint(2, 9) / 100.0:.2f}",
        'QUANTITY': random.randint(24, 25)
    }


def get_q7_inputs():
    nations_sample = random.sample(NATIONS, 2)
    return {'NATION1': nations_sample[0], 'NATION2': nations_sample[1]}

def get_q8_inputs():
    nation = random.choice(NATIONS)
    region = NATION_TO_REGION[nation]
    p_type = f"{random.choice(TYPES_S1)} {random.choice(TYPES_S2)} {random.choice(TYPES_S3)}"
    return {'NATION': nation, 'REGION': region, 'TYPE': p_type}

def get_q9_inputs():
    return {'COLOR': random.choice(P_NAME_COLORS)}

def get_q10_inputs():
    # A regra é do segundo mês de 1993 até o primeiro de 1995.
    start = date(1993, 2, 1)
    end = date(1995, 1, 31) # Fim do período para seleção do mês
    total_months = (end.year - start.year) * 12 + end.month - start.month
    rand_month_offset = random.randint(0, total_months -1)
    year = start.year + (start.month + rand_month_offset -1) // 12
    month = (start.month + rand_month_offset -1) % 12 + 1
    return {'DATE': date(year, month, 1).strftime("%Y-%m-%d")}

def get_q11_inputs(sf=1):
    return {
        'NATION': random.choice(NATIONS),
        'FRACTION': 0.0001 / sf
    }

def get_q12_inputs():
    modes_sample = random.sample(MODES, 2)
    return {
        'SHIPMODE1': modes_sample[0],
        'SHIPMODE2': modes_sample[1],
        'DATE': date(random.randint(1993, 1997), 1, 1).strftime("%Y-%m-%d")
    }

def get_q13_inputs():
    return {'WORD1': random.choice(WORDS_1), 'WORD2': random.choice(WORDS_2)}

def get_q14_inputs():
    year = random.randint(1993, 1997)
    month = random.randint(1, 12)
    return {'DATE': date(year, month, 1).strftime("%Y-%m-%d")}

def get_q15_inputs():
    year = random.randint(1993, 1997)
    month = random.randint(1, 10) if year == 1997 else random.randint(1, 12)
    return {'DATE': date(year, month, 1).strftime("%Y-%m-%d")}

def get_q16_inputs():
    brand = f"Brand#{random.randint(1, 5)}{random.randint(1, 5)}"
    p_type = f"{random.choice(TYPES_S1)} {random.choice(TYPES_S2)}"
    sizes = random.sample(range(1, 51), 8)
    # O retorno pode ser um dicionário com uma lista ou chaves individuais
    return {'BRAND': brand, 'TYPE': p_type, 'SIZES': sizes}

def get_q17_inputs():
    brand = f"Brand#{random.randint(1, 5)}{random.randint(1, 5)}"
    container = f"{random.choice(CONTAINERS_S1)} {random.choice(CONTAINERS_S2)}"
    return {'BRAND': brand, 'CONTAINER': container}

def get_q18_inputs():
    return {'QUANTITY': random.randint(312, 315)}


def get_q19_inputs():
    def gen_brand():
        return f"Brand#{random.randint(1, 5)}{random.randint(1, 5)}"

    return {
        'QUANTITY1': random.randint(1, 10),
        'QUANTITY2': random.randint(10, 20),
        'QUANTITY3': random.randint(20, 30),
        'BRAND1': gen_brand(),
        'BRAND2': gen_brand(),
        'BRAND3': gen_brand()
    }

def get_q20_inputs():
    return {
        'COLOR': random.choice(P_NAME_COLORS),
        'DATE': date(random.randint(1993, 1997), 1, 1).strftime("%Y-%m-%d"),
        'NATION': random.choice(NATIONS)
    }

def get_q21_inputs():
    return {'NATION': random.choice(NATIONS)}

def get_q22_inputs():
    # Country codes são de 10 a 34 para as 25 nações.
    country_codes = random.sample(range(10, 35), 7)
    return {'CODES': [str(code) for code in country_codes]}


INPUT_GENERATORS = {
    1: get_q1_inputs,
    2: get_q2_inputs,
    3: get_q3_inputs,
    4: get_q4_inputs,
    5: get_q5_inputs,
    6: get_q6_inputs,
    7: get_q7_inputs,
    8: get_q8_inputs,
    9: get_q9_inputs,
    10: get_q10_inputs,
    11: get_q11_inputs,
    12: get_q12_inputs,
    13: get_q13_inputs,
    14: get_q14_inputs,
    15: get_q15_inputs,
    16: get_q16_inputs,
    17: get_q17_inputs,
    18: get_q18_inputs,
    19: get_q19_inputs,
    20: get_q20_inputs,
    21: get_q21_inputs,
    22: get_q22_inputs,
}