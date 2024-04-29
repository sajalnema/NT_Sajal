def split(line):
    words = line.split()
    return words

def mapper(line):
    dict = {}
    for word in split(line):
        if word in dict:
            dict[f'{word}'] += 1
        else:
            dict[f'{word}'] = 1
    return dict

def reducer(mapped_dict_list):
    reduced_dict = {}
    for dict in mapped_dict_list:
        for key, value in dict.items():
            if key in reduced_dict:
                reduced_dict[key] += value
            else:
                reduced_dict[key] = value
    return reduced_dict


with open(r'C:\Studies\Use Cases\Map Reduce Assignment.txt', 'r') as file:
    
    lines = file.read().splitlines()

    mapped_dict_list = []
    for line in lines:
        mapped_dict_list.append(mapper(line)) # map all the lines

    reduced_dict = reducer(mapped_dict_list) # reduce the list of all mapped dictionaries

    print(reduced_dict) #final reduded dictionary