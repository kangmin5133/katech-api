"""change vehicleInfo.terminal_info to unique key

Revision ID: 3a4e9a80de08
Revises: 9816c75f7152
Create Date: 2023-11-02 10:44:31.334404

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '3a4e9a80de08'
down_revision: Union[str, None] = '9816c75f7152'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_unique_constraint(None, 'vehicle_info', ['terminal_info'])
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint(None, 'vehicle_info', type_='unique')
    # ### end Alembic commands ###
