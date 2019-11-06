from django.db import models


class Diagnosis(models.Model):
    when = models.DateTimeField()
    sex = models.CharField(max_length=1, default='M')
    icd10 = models.CharField(max_length=20)
