import struct


def pack_tag(data_type, tag_schemaid, data_schemaid, tag_data, data):
    """
    ----------------------------------------
    |data_type(2 Bytes)|tag_length(2 Bytes)|
    |       data_length(4 Bytes)           |
    |       tag_schemaid(4 Bytes)          |
    |       data_schemaid(4 Bytes)         |(16 Bytes header)
    |--------------------------------------|
    |       tag_data(unknow length)        |
    |       data(unknow length)            |
    |______________________________________|
    """
    tag_length, data_length = len(tag_data), struct.calcsize('<2H3I')+len(tag_data)+len(data)
    header = struct.pack('<2H3I', data_type, tag_length, data_length, tag_schemaid, data_schemaid)
    return header + tag_data + data


def unpack_tag(bin_data):
    """
    bin_data : binary data 
    return :
    header -> tuple(data_type, tag_length, data_length, tag_schemaid, data_schemaid)
    tag_data -> binary 
    data -> binary
    """
    header_size = struct.calcsize('<2H3I')
    header = struct.unpack('<2H3I', bin_data[:header_size])
    return header, bin_data[header_size: header_size+header[1]], bin_data[header_size+header[1]:]


if __name__ == '__main__':
    seri = pack_tag(2, 4, 10, 'avsggase', 'hhdzxczhx')
    print(seri)
    print((unpack_tag(seri)))
