import math
from pyspark import SparkContext


def to_wgs84(x, y):
    if (abs(x) > 20037508.3427892) or (abs(y) > 20037508.3427892):
        return 0, 0

    rad = 6378137.0
    lat = (1.5707963267948966 - (2.0 * math.atan(math.exp((-1.0 * y) / rad)))) * (180 / math.pi)
    lon = ((x / rad) * 57.295779513082323) - (
        (math.floor((((x / rad) * 57.295779513082323) + 180.0) / 360.0)) * 360.0)

    return lon, lat


def to_web_mercator(lon, lat):
    if (abs(lon) > 180) or (abs(lat) > 89):
        return 0, 0

    rad = 6378137.0
    east = lon * 0.017453292519943295
    north = lat * 0.017453292519943295

    y = 3189068.5 * math.log((1.0 + math.sin(north)) / (1.0 - math.sin(north)))
    x = rad * east

    return x, y


def line_to_row_col(line_, cell_size_):
    splits = line_.split(',')
    try:
        c = int(math.floor(float(splits[10]) / cell_size_))
        r = int(math.floor(float(splits[11]) / cell_size_))
        return (r, c), 1
    except Exception:
        return (0, 0), -1


def row_col_to_xy(t, cell_size_, cell_half_):
    ((row, col), count) = t
    y = row * cell_size_ + cell_half_
    x = col * cell_size_ + cell_half_
    return "{0},{1},{2}".format(x, y, count)


if __name__ == "__main__":
    sc = SparkContext()
    hc = sc._jsc.hadoopConfiguration()
    conf = sc.getConf()
    start = len("spark.service.user.")
    for k, v in conf.getAll():
        if k.startswith("spark.service.user."):
            hc.set(k[start:], v)
    inp_path = conf.get("spark.service.user.input.path", "swift2d://trips.thunder/trips-1M.csv")
    out_path = conf.get("spark.service.user.output.path", "swift2d://output.thunder/bins")
    cell_size = float(conf.get("spark.service.user.cell.size", "0.001"))
    cell_half = cell_size * 0.5
    sc.textFile(inp_path). \
        map(lambda line: line_to_row_col(line, cell_size)). \
        filter(lambda t: t[1] > 0). \
        reduceByKey(lambda a, b: a + b). \
        filter(lambda t: t[1] > 2). \
        map(lambda t: row_col_to_xy(t, cell_size, cell_half)). \
        saveAsTextFile(out_path)
