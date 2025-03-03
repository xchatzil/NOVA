import seaborn as sns


def get_approaches():
    return ['sink-based', 'source-based', 'top-c', 'tree', 'cl-sf', 'cl-tree-sf', 'nova']


def get_eval_colors_dict():
    ecolors = sns.color_palette(n_colors=10)
    return {
        "source-based": ecolors[0],
        "sink-based": ecolors[1],
        "tree": ecolors[2],
        'cl-sf': ecolors[3],
        'cl-tree-sf': ecolors[4],
        'top-c': ecolors[5],
        "nova": ecolors[6],
        "nova (p)": "magenta",
        "nova (re-opt)": "magenta",
    }


def get_markers_dict():
    # markers = ["o", "v", "^", "<", ">", "s", "p", "*", "h", "D"]
    return {
        "source-based": "1",
        "sink-based": "2",
        'top-c': "3",
        "tree": "o",
        'cl-sf': "<",
        'cl-tree-sf': ">",
        "nova": "*",
        "nova (re-opt)": "x",
    }


def get_styles_dict():
    return {
        "source-based": "-",  # Solid line
        "sink-based": "--",  # Dashed line
        'top-c': "-.",  # Dash-dot line
        "tree": ":",  # Dotted line
        'cl-sf': "-",  # Solid line again (to repeat)
        'cl-tree-sf': "--",  # Dashed line again (to repeat)
        "nova": (0, (3, 1, 1, 1)),  # Dash-dot-dot line
        "nova (re-opt)": (0, (3, 1, 1, 1)),  # Dash-dot-dot line
        # (0, (5, 1)),  # Long dash
        # (0, (1, 1)),  # Dense dots
        # (0, (4, 2, 1, 2)),  # Mixed dash-dot-dot
    }
