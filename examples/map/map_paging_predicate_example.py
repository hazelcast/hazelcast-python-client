import hazelcast

from hazelcast.serialization.predicate import is_greater_than_or_equal_to, PagingPredicate

if __name__ == "__main__":
    client = hazelcast.HazelcastClient()

    student_grades_map = client.get_map('student grades').blocking()
    student_grades_map.put_all({"student" + str(i): 100-i for i in range(10)})

    greater_equal_predicate = is_greater_than_or_equal_to('this', 94)  # Query for student grades that are >= 94.
    paging_predicate = PagingPredicate(greater_equal_predicate, 2)  # Page size 2.

    # Retrieve first page:
    grades_first_page = student_grades_map.values(paging_predicate)  # [94, 95]

    # ...
    # Set up next page:
    paging_predicate.next_page()

    # Retrieve next page:
    grades_second_page = student_grades_map.values(paging_predicate)  # [96, 97]

    # Set page to fourth page and retrieve (page index = page no - 1):
    paging_predicate.page = 3
    grades_fourth_page = student_grades_map.values(paging_predicate)  # [100]
