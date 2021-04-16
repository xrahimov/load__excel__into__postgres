cnt = 0
def count_nodes(head):
    while head.next:
        cnt += 1

    return cnt
