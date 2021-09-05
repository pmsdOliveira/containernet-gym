from typing import List


DURATION_TEMPLATES: List[int] = [1, 5, 10, 20, 30, 40, 50, 60]
BW_TEMPLATES: List[float] = [1.0, 5.0, 10.0, 20.0, 40.0, 50.0, 60.0, 80.0, 100.0]
IE_PRICE_RATIO: float = 5.0


if __name__ == '__main__':
    elastic_templates: List[str] = []
    inelastic_templates: List[str] = []
    for i in range(len(DURATION_TEMPLATES)):
        for j in range(len(BW_TEMPLATES)):
            elastic_templates += [f'e\t\t{DURATION_TEMPLATES[i]}\t\t\t\t{BW_TEMPLATES[j]}'
                                  f'\t\t\t{BW_TEMPLATES[j] / 100:.2f}']
            inelastic_templates += [f'i\t\t{DURATION_TEMPLATES[i]}\t\t\t\t{BW_TEMPLATES[j]}'
                                    f'\t\t\t{IE_PRICE_RATIO * BW_TEMPLATES[j] / 100:.2f}']

    with open("request_templates.txt", 'w') as f:
        f.write('type\tduration (s)\tbw (Mb/s)\tprice (euros/s)\n')
        f.write('---------------------------------------------------\n')
        for (elastic, inelastic) in zip(elastic_templates, inelastic_templates):
            f.write(f'{elastic}\n')
            f.write(f'{inelastic}\n')
